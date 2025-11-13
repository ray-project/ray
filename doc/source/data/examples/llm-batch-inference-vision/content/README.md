<!--
Do not modify this README. This file is a copy of the notebook and is not used to display the content.
Modify llm_batch_inference_vision.ipynb instead, then regenerate this file with:
jupyter nbconvert llm_batch_inference_vision.ipynb --to markdown --output README.md
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

Ray Data LLM runs batch inference for VLMs on Ray Data datasets containing images. In this tutorial, you perform batch inference with a vision-language model to generate image captions.

You'll use the Docmatix dataset, which contains millions of document images with rich annotations. The dataset is available on Hugging Face and designed for document understanding tasks.

First, load the data from Hugging Face using streaming mode. Then, to ensure the workload can be distributed across multiple GPUs, repartition the dataset.



```python
%pip install datasets
```

    Requirement already satisfied: datasets in /home/ray/anaconda3/lib/python3.11/site-packages (4.4.1)
    Requirement already satisfied: filelock in /home/ray/anaconda3/lib/python3.11/site-packages (from datasets) (3.17.0)
    Requirement already satisfied: numpy>=1.17 in /home/ray/anaconda3/lib/python3.11/site-packages (from datasets) (1.26.4)
    Requirement already satisfied: pyarrow>=21.0.0 in /home/ray/anaconda3/lib/python3.11/site-packages (from datasets) (22.0.0)
    Requirement already satisfied: dill<0.4.1,>=0.3.0 in /home/ray/anaconda3/lib/python3.11/site-packages (from datasets) (0.3.8)
    Requirement already satisfied: pandas in /home/ray/anaconda3/lib/python3.11/site-packages (from datasets) (1.5.3)
    Requirement already satisfied: requests>=2.32.2 in /home/ray/anaconda3/lib/python3.11/site-packages (from datasets) (2.32.3)
    Requirement already satisfied: httpx<1.0.0 in /home/ray/anaconda3/lib/python3.11/site-packages (from datasets) (0.28.1)
    Requirement already satisfied: tqdm>=4.66.3 in /home/ray/anaconda3/lib/python3.11/site-packages (from datasets) (4.67.1)
    Requirement already satisfied: xxhash in /home/ray/anaconda3/lib/python3.11/site-packages (from datasets) (3.6.0)
    Requirement already satisfied: multiprocess<0.70.19 in /home/ray/anaconda3/lib/python3.11/site-packages (from datasets) (0.70.16)
    Requirement already satisfied: fsspec<=2025.10.0,>=2023.1.0 in /home/ray/anaconda3/lib/python3.11/site-packages (from fsspec[http]<=2025.10.0,>=2023.1.0->datasets) (2023.12.1)
    Requirement already satisfied: huggingface-hub<2.0,>=0.25.0 in /home/ray/anaconda3/lib/python3.11/site-packages (from datasets) (0.34.3)
    Requirement already satisfied: packaging in /home/ray/anaconda3/lib/python3.11/site-packages (from datasets) (23.0)
    Requirement already satisfied: pyyaml>=5.1 in /home/ray/anaconda3/lib/python3.11/site-packages (from datasets) (6.0.1)
    Requirement already satisfied: aiohttp!=4.0.0a0,!=4.0.0a1 in /home/ray/anaconda3/lib/python3.11/site-packages (from fsspec[http]<=2025.10.0,>=2023.1.0->datasets) (3.11.16)
    Requirement already satisfied: anyio in /home/ray/anaconda3/lib/python3.11/site-packages (from httpx<1.0.0->datasets) (3.7.1)
    Requirement already satisfied: certifi in /home/ray/anaconda3/lib/python3.11/site-packages (from httpx<1.0.0->datasets) (2025.1.31)
    Requirement already satisfied: httpcore==1.* in /home/ray/anaconda3/lib/python3.11/site-packages (from httpx<1.0.0->datasets) (1.0.9)
    Requirement already satisfied: idna in /home/ray/anaconda3/lib/python3.11/site-packages (from httpx<1.0.0->datasets) (3.7)
    Requirement already satisfied: h11>=0.16 in /home/ray/anaconda3/lib/python3.11/site-packages (from httpcore==1.*->httpx<1.0.0->datasets) (0.16.0)
    Requirement already satisfied: typing-extensions>=3.7.4.3 in /home/ray/anaconda3/lib/python3.11/site-packages (from huggingface-hub<2.0,>=0.25.0->datasets) (4.12.2)
    Requirement already satisfied: hf-xet<2.0.0,>=1.1.3 in /home/ray/anaconda3/lib/python3.11/site-packages (from huggingface-hub<2.0,>=0.25.0->datasets) (1.1.3)
    Requirement already satisfied: charset_normalizer<4,>=2 in /home/ray/anaconda3/lib/python3.11/site-packages (from requests>=2.32.2->datasets) (3.3.2)
    Requirement already satisfied: urllib3<3,>=1.21.1 in /home/ray/anaconda3/lib/python3.11/site-packages (from requests>=2.32.2->datasets) (1.26.19)
    Requirement already satisfied: python-dateutil>=2.8.1 in /home/ray/anaconda3/lib/python3.11/site-packages (from pandas->datasets) (2.8.2)
    Requirement already satisfied: pytz>=2020.1 in /home/ray/anaconda3/lib/python3.11/site-packages (from pandas->datasets) (2022.7.1)
    Requirement already satisfied: aiohappyeyeballs>=2.3.0 in /home/ray/anaconda3/lib/python3.11/site-packages (from aiohttp!=4.0.0a0,!=4.0.0a1->fsspec[http]<=2025.10.0,>=2023.1.0->datasets) (2.6.1)
    Requirement already satisfied: aiosignal>=1.1.2 in /home/ray/anaconda3/lib/python3.11/site-packages (from aiohttp!=4.0.0a0,!=4.0.0a1->fsspec[http]<=2025.10.0,>=2023.1.0->datasets) (1.3.1)
    Requirement already satisfied: attrs>=17.3.0 in /home/ray/anaconda3/lib/python3.11/site-packages (from aiohttp!=4.0.0a0,!=4.0.0a1->fsspec[http]<=2025.10.0,>=2023.1.0->datasets) (25.1.0)
    Requirement already satisfied: frozenlist>=1.1.1 in /home/ray/anaconda3/lib/python3.11/site-packages (from aiohttp!=4.0.0a0,!=4.0.0a1->fsspec[http]<=2025.10.0,>=2023.1.0->datasets) (1.4.1)
    Requirement already satisfied: multidict<7.0,>=4.5 in /home/ray/anaconda3/lib/python3.11/site-packages (from aiohttp!=4.0.0a0,!=4.0.0a1->fsspec[http]<=2025.10.0,>=2023.1.0->datasets) (6.0.5)
    Requirement already satisfied: propcache>=0.2.0 in /home/ray/anaconda3/lib/python3.11/site-packages (from aiohttp!=4.0.0a0,!=4.0.0a1->fsspec[http]<=2025.10.0,>=2023.1.0->datasets) (0.3.0)
    Requirement already satisfied: yarl<2.0,>=1.17.0 in /home/ray/anaconda3/lib/python3.11/site-packages (from aiohttp!=4.0.0a0,!=4.0.0a1->fsspec[http]<=2025.10.0,>=2023.1.0->datasets) (1.18.3)
    Requirement already satisfied: six>=1.5 in /home/ray/anaconda3/lib/python3.11/site-packages (from python-dateutil>=2.8.1->pandas->datasets) (1.16.0)
    Requirement already satisfied: sniffio>=1.1 in /home/ray/anaconda3/lib/python3.11/site-packages (from anyio->httpx<1.0.0->datasets) (1.3.1)
    Note: you may need to restart the kernel to use updated packages.



```python
import ray
import datasets
from PIL import Image
from io import BytesIO

# Load the Docmatix dataset from Hugging Face.
# This dataset contains images with document understanding annotations.
print("Loading Docmatix dataset from Hugging Face...")
hf_dataset = datasets.load_dataset("HuggingFaceM4/Docmatix", "images", split="train", streaming=True)

# Convert to Ray Dataset.
ds = ray.data.from_huggingface(hf_dataset)

print("Dataset loaded successfully.")

print("Sample data:")
sample = ds.take(2)
for i, item in enumerate(sample):
    print(f"\nSample {i+1}:")
    print(f"  - Fields: {list(item.keys())}")
    # Check for image field
    if 'image' in item:
        print(f"  - Has image field: True")
        print(f"  - Image type: {type(item['image'])}")
```

    Loading Leopard-Instruct dataset from Hugging Face...



    Resolving data files:   0%|          | 0/100 [00:00<?, ?it/s]


    2025-11-12 23:21:57,846	INFO logging.py:397 -- Registered dataset logger for dataset dataset_155_0
    2025-11-12 23:21:57,850	INFO streaming_executor.py:170 -- Starting execution of Dataset dataset_155_0. Full logs are in /tmp/ray/session_2025-11-12_14-59-56_310597_2440/logs/ray-data
    2025-11-12 23:21:57,852	INFO streaming_executor.py:171 -- Execution plan of Dataset dataset_155_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ReadHuggingFace] -> LimitOperator[limit=2]


    Dataset loaded successfully.
    Sample data:



    Running 0: 0.00 row [00:00, ? row/s]



    - ReadHuggingFace->SplitBlocks(200) 1: 0.00 row [00:00, ? row/s]



    - limit=2 2: 0.00 row [00:00, ? row/s]


    2025-11-12 23:22:10,411	INFO streaming_executor.py:298 -- ✔️  Dataset dataset_155_0 execution finished in 12.56 seconds
    2025-11-12 23:22:10,421	INFO util.py:257 -- Exiting prefetcher's background thread


    
    Sample 1:
      - Has images: True



    
![png](README_files/README_3_9.png)
    


    
    Sample 2:
      - Has images: True



    
![png](README_files/README_3_11.png)
    


For this initial example, limit the dataset to 10,000 rows for faster processing and testing. Later, you can scale up to process the full dataset.


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

    Limiting dataset to 10,000 images for initial processing.
    Repartitioning dataset into 64 blocks for parallelism...


## Configure Ray Data LLM

Ray Data LLM provides a unified interface to run batch inference with different VLM engines. Configure the vLLM engine with a vision-language model, define preprocessing and postprocessing functions, and build the processor.

### Configure the processor engine

Configure the model and compute resources needed for inference using `vLLMEngineProcessorConfig` with vision support enabled.

This example uses the `Qwen/Qwen2.5-VL-3B-Instruct` model, a vision-language model. The configuration specifies:
- `model_source`: The Hugging Face model identifier.
- `engine_kwargs`: vLLM engine parameters such as memory settings and batching.
- `runtime_env`: Environment variables needed for the vLLM vision API (V1).
- `batch_size`: Number of requests to batch together (set to 16 for vision models).
- `accelerator_type`: GPU type to use (L4 in this case).
- `concurrency`: Number of parallel workers (4 replicas).
- `has_image`: Enable image input support.

**Note:** Vision models typically require smaller batch sizes than text-only models due to the additional memory needed for image processing. Adjust batch size based on your image resolution and GPU memory.



```python
from ray.data.llm import vLLMEngineProcessorConfig

MAX_MODEL_LEN = 8192

processor_config = vLLMEngineProcessorConfig(
    model_source="Qwen/Qwen2.5-VL-3B-Instruct",
    engine_kwargs=dict(
        max_model_len=MAX_MODEL_LEN,
        max_num_batched_tokens=2048,
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
    # Get the image from the row
    image = row.get('image')
    
    # Convert to PIL Image if needed
    if not isinstance(image, Image.Image):
        if isinstance(image, dict) and 'bytes' in image:
            image = Image.open(BytesIO(image['bytes']))
        elif isinstance(image, bytes):
            image = Image.open(BytesIO(image))
    
    # Resize for consistency
    if image:
        image = image.resize((224, 224), Image.Resampling.BICUBIC)
    
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
from pprint import pprint

# Build the LLM processor with the configuration and functions.
processor = build_llm_processor(
    processor_config,
    preprocess=preprocess,
    postprocess=postprocess,
)
```

    2025-11-13 00:46:23,107	INFO worker.py:1832 -- Connecting to existing Ray cluster at address: 10.0.96.46:6379...
    2025-11-13 00:46:23,108	INFO worker.py:1850 -- Calling ray.init() again after it has already been called.
    No cloud storage mirror configured


## Process the dataset

Run the processor on your small dataset to perform batch inference. Ray Data automatically distributes the workload across available GPUs and handles batching, retries, and resource management.



```python
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

    2025-11-13 00:46:24,667	WARNING util.py:597 -- The argument ``concurrency`` is deprecated in Ray 2.51. Please specify argument ``compute`` instead. For more information, see https://docs.ray.io/en/master/data/transforming-data.html#stateful-transforms.
    2025-11-13 00:46:24,669	WARNING util.py:597 -- The argument ``concurrency`` is deprecated in Ray 2.51. Please specify argument ``compute`` instead. For more information, see https://docs.ray.io/en/master/data/transforming-data.html#stateful-transforms.
    2025-11-13 00:46:24,670	WARNING util.py:597 -- The argument ``concurrency`` is deprecated in Ray 2.51. Please specify argument ``compute`` instead. For more information, see https://docs.ray.io/en/master/data/transforming-data.html#stateful-transforms.
    2025-11-13 00:46:24,673	WARNING util.py:597 -- The argument ``concurrency`` is deprecated in Ray 2.51. Please specify argument ``compute`` instead. For more information, see https://docs.ray.io/en/master/data/transforming-data.html#stateful-transforms.
    2025-11-13 00:46:24,679	INFO logging.py:397 -- Registered dataset logger for dataset dataset_213_0
    2025-11-13 00:46:24,686	INFO streaming_executor.py:170 -- Starting execution of Dataset dataset_213_0. Full logs are in /tmp/ray/session_2025-11-12_14-59-56_310597_2440/logs/ray-data
    2025-11-13 00:46:24,687	INFO streaming_executor.py:171 -- Execution plan of Dataset dataset_213_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ReadHuggingFace] -> LimitOperator[limit=10000] -> AllToAllOperator[Repartition] -> ActorPoolMapOperator[Map(_preprocess)->MapBatches(PrepareImageUDF)] -> ActorPoolMapOperator[MapBatches(ChatTemplateUDF)] -> ActorPoolMapOperator[MapBatches(TokenizeUDF)] -> ActorPoolMapOperator[MapBatches(vLLMEngineStageUDF)] -> ActorPoolMapOperator[MapBatches(DetokenizeUDF)] -> TaskPoolMapOperator[Map(_postprocess)]



    Running 0: 0.00 row [00:00, ? row/s]



    - ReadHuggingFace->SplitBlocks(200) 1: 0.00 row [00:00, ? row/s]



    - limit=10000 2: 0.00 row [00:00, ? row/s]



    - Repartition 3: 0.00 row [00:00, ? row/s]



    Split Repartition 4:   0%|          | 0.00/1.00 [00:00<?, ? row/s]



    - Map(_preprocess)->MapBatches(PrepareImageUDF) 5: 0.00 row [00:00, ? row/s]



    - MapBatches(ChatTemplateUDF) 6: 0.00 row [00:00, ? row/s]



    - MapBatches(TokenizeUDF) 7: 0.00 row [00:00, ? row/s]



    - MapBatches(vLLMEngineStageUDF) 8: 0.00 row [00:00, ? row/s]



    - MapBatches(DetokenizeUDF) 9: 0.00 row [00:00, ? row/s]



    - Map(_postprocess) 10: 0.00 row [00:00, ? row/s]


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m Max pending requests is set to 141
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m Downloading model and tokenizer.
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m No cloud storage mirror configured


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m INFO 11-13 00:46:29 [__init__.py:216] Automatically detected platform cuda.


    [36m(MapWorker(MapBatches(ChatTemplateUDF)) pid=32158, ip=10.0.67.56)[0m The image processor of type `Qwen2VLImageProcessor` is now loaded as a fast processor by default, even if the model checkpoint was saved with a slow processor. This is a breaking change and may produce slightly different outputs. To continue using the slow processor, instantiate this class with `use_fast=False`. Note that this behavior will be extended to all models in a future release.
    [36m(MapWorker(MapBatches(ChatTemplateUDF)) pid=32158, ip=10.0.67.56)[0m You have video processor config saved in `preprocessor.json` file which is deprecated. Video processor configs should be saved in their own `video_preprocessor.json` file. You can rename the file or load and save the processor back which renames it automatically. Loading from `preprocessor.json` will be removed in v5.0.


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m INFO 11-13 00:46:31 [model.py:547] Resolved architecture: Qwen2_5_VLForConditionalGeneration
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m INFO 11-13 00:46:31 [model.py:1510] Using max model len 8192
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m INFO 11-13 00:46:31 [arg_utils.py:1215] Using ray runtime env: {'_ray_commit': 'aceca04cbbf42e2c50a7e523108a35961c1a612e', 'cgroupv2': {}, 'ray_debugger': {'working_dir': '/home/ray/default'}, 'working_dir': 'gcs://_ray_pkg_71ce8c148e2944680e6a5fd1071541790989e709.zip', 'pip': {'packages': ['datasets'], 'pip_check': False}, 'env_vars': {'LARGE_DATASET_LIMIT': '10000'}}
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m INFO 11-13 00:46:31 [scheduler.py:205] Chunked prefill is enabled with max_num_batched_tokens=2048.
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m WARNING 11-13 00:46:34 [__init__.py:3036] We must use the `spawn` multiprocessing start method. Overriding VLLM_WORKER_MULTIPROC_METHOD to 'spawn'. See https://docs.vllm.ai/en/latest/usage/troubleshooting.html#python-multiprocessing for more information. Reasons: In a Ray actor and can only be spawned
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m INFO 11-13 00:46:29 [__init__.py:216] Automatically detected platform cuda.[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m INFO 11-13 00:46:32 [model.py:547] Resolved architecture: Qwen2_5_VLForConditionalGeneration[32m [repeated 7x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m INFO 11-13 00:46:32 [model.py:1510] Using max model len 8192[32m [repeated 7x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m INFO 11-13 00:46:32 [arg_utils.py:1215] Using ray runtime env: {'_ray_commit': 'aceca04cbbf42e2c50a7e523108a35961c1a612e', 'cgroupv2': {}, 'ray_debugger': {'working_dir': '/home/ray/default'}, 'working_dir': 'gcs://_ray_pkg_71ce8c148e2944680e6a5fd1071541790989e709.zip', 'pip': {'packages': ['datasets'], 'pip_check': False}, 'env_vars': {'LARGE_DATASET_LIMIT': '10000'}}[32m [repeated 7x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m INFO 11-13 00:46:32 [scheduler.py:205] Chunked prefill is enabled with max_num_batched_tokens=2048.[32m [repeated 7x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m [1;36m(EngineCore_DP0 pid=35943)[0;0m INFO 11-13 00:46:39 [core.py:644] Waiting for init message from front-end.
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [1;36m(EngineCore_DP0 pid=41340)[0;0m INFO 11-13 00:46:39 [core.py:77] Initializing a V1 LLM engine (v0.11.0) with config: model='Qwen/Qwen2.5-VL-3B-Instruct', speculative_config=None, tokenizer='Qwen/Qwen2.5-VL-3B-Instruct', skip_tokenizer_init=False, tokenizer_mode=auto, revision=None, tokenizer_revision=None, trust_remote_code=False, dtype=torch.bfloat16, max_seq_len=8192, download_dir=None, load_format=auto, tensor_parallel_size=1, pipeline_parallel_size=1, data_parallel_size=1, disable_custom_all_reduce=False, quantization=None, enforce_eager=False, kv_cache_dtype=auto, device_config=cuda, structured_outputs_config=StructuredOutputsConfig(backend='auto', disable_fallback=False, disable_any_whitespace=False, disable_additional_properties=False, reasoning_parser=''), observability_config=ObservabilityConfig(show_hidden_metrics_for_version=None, otlp_traces_endpoint=None, collect_detailed_traces=None), seed=0, served_model_name=Qwen/Qwen2.5-VL-3B-Instruct, enable_prefix_caching=True, chunked_prefill_enabled=True, pooler_config=None, compilation_config={"level":3,"debug_dump_path":"","cache_dir":"","backend":"","custom_ops":[],"splitting_ops":["vllm.unified_attention","vllm.unified_attention_with_output","vllm.mamba_mixer2","vllm.mamba_mixer","vllm.short_conv","vllm.linear_attention","vllm.plamo2_mamba_mixer","vllm.gdn_attention","vllm.sparse_attn_indexer"],"use_inductor":true,"compile_sizes":[],"inductor_compile_config":{"enable_auto_functionalized_v2":false},"inductor_passes":{},"cudagraph_mode":[2,1],"use_cudagraph":true,"cudagraph_num_of_warmups":1,"cudagraph_capture_sizes":[256,248,240,232,224,216,208,200,192,184,176,168,160,152,144,136,128,120,112,104,96,88,80,72,64,56,48,40,32,24,16,8,4,2,1],"cudagraph_copy_inputs":false,"full_cuda_graph":false,"use_inductor_graph_partition":false,"pass_config":{},"max_capture_size":256,"local_cache_dir":null}
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [1;36m(EngineCore_DP0 pid=41340)[0;0m INFO 11-13 00:46:39 [shm_broadcast.py:289] vLLM message queue communication handle: Handle(local_reader_ranks=[0], buffer_handle=(1, 16777216, 10, 'psm_ef9c78a7'), local_subscribe_addr='ipc:///tmp/137efa58-7d57-4670-9bef-3f2651f970dc', remote_subscribe_addr=None, remote_addr_ipv6=False)
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m WARNING 11-13 00:46:35 [__init__.py:3036] We must use the `spawn` multiprocessing start method. Overriding VLLM_WORKER_MULTIPROC_METHOD to 'spawn'. See https://docs.vllm.ai/en/latest/usage/troubleshooting.html#python-multiprocessing for more information. Reasons: In a Ray actor and can only be spawned[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m INFO 11-13 00:46:38 [__init__.py:216] Automatically detected platform cuda.[32m [repeated 4x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m [1;36m(EngineCore_DP0 pid=41541)[0;0m INFO 11-13 00:46:39 [core.py:644] Waiting for init message from front-end.[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m [1;36m(EngineCore_DP0 pid=41541)[0;0m INFO 11-13 00:46:39 [core.py:77] Initializing a V1 LLM engine (v0.11.0) with config: model='Qwen/Qwen2.5-VL-3B-Instruct', speculative_config=None, tokenizer='Qwen/Qwen2.5-VL-3B-Instruct', skip_tokenizer_init=False, tokenizer_mode=auto, revision=None, tokenizer_revision=None, trust_remote_code=False, dtype=torch.bfloat16, max_seq_len=8192, download_dir=None, load_format=auto, tensor_parallel_size=1, pipeline_parallel_size=1, data_parallel_size=1, disable_custom_all_reduce=False, quantization=None, enforce_eager=False, kv_cache_dtype=auto, device_config=cuda, structured_outputs_config=StructuredOutputsConfig(backend='auto', disable_fallback=False, disable_any_whitespace=False, disable_additional_properties=False, reasoning_parser=''), observability_config=ObservabilityConfig(show_hidden_metrics_for_version=None, otlp_traces_endpoint=None, collect_detailed_traces=None), seed=0, served_model_name=Qwen/Qwen2.5-VL-3B-Instruct, enable_prefix_caching=True, chunked_prefill_enabled=True, pooler_config=None, compilation_config={"level":3,"debug_dump_path":"","cache_dir":"","backend":"","custom_ops":[],"splitting_ops":["vllm.unified_attention","vllm.unified_attention_with_output","vllm.mamba_mixer2","vllm.mamba_mixer","vllm.short_conv","vllm.linear_attention","vllm.plamo2_mamba_mixer","vllm.gdn_attention","vllm.sparse_attn_indexer"],"use_inductor":true,"compile_sizes":[],"inductor_compile_config":{"enable_auto_functionalized_v2":false},"inductor_passes":{},"cudagraph_mode":[2,1],"use_cudagraph":true,"cudagraph_num_of_warmups":1,"cudagraph_capture_sizes":[256,248,240,232,224,216,208,200,192,184,176,168,160,152,144,136,128,120,112,104,96,88,80,72,64,56,48,40,32,24,16,8,4,2,1],"cudagraph_copy_inputs":false,"full_cuda_graph":false,"use_inductor_graph_partition":false,"pass_config":{},"max_capture_size":256,"local_cache_dir":null}[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m INFO 11-13 00:46:45 [shm_broadcast.py:289] vLLM message queue communication handle: Handle(local_reader_ranks=[0], buffer_handle=(1, 10485760, 10, 'psm_92cc1c28'), local_subscribe_addr='ipc:///tmp/b2f4a5ba-73e4-452a-995e-8659d6cb38ad', remote_subscribe_addr=None, remote_addr_ipv6=False)[32m [repeated 4x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m INFO 11-13 00:46:42 [__init__.py:216] Automatically detected platform cuda.[32m [repeated 4x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [Gloo] Rank 0 is connected to 0 peer ranks. Expected number of connected peer ranks is : 0
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [Gloo] Rank 0 is connected to 0 peer ranks. Expected number of connected peer ranks is : 0
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [Gloo] Rank 0 is connected to 0 peer ranks. Expected number of connected peer ranks is : 0
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [Gloo] Rank 0 is connected to 0 peer ranks. Expected number of connected peer ranks is : 0
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [Gloo] Rank 0 is connected to 0 peer ranks. Expected number of connected peer ranks is : 0
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [Gloo] Rank 0 is connected to 0 peer ranks. Expected number of connected peer ranks is : 0
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m INFO 11-13 00:46:45 [parallel_state.py:1208] rank 0 in world size 1 is assigned as DP rank 0, PP rank 0, TP rank 0, EP rank 0
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m WARNING 11-13 00:46:46 [topk_topp_sampler.py:66] FlashInfer is not available. Falling back to the PyTorch-native implementation of top-p & top-k sampling. For the best performance, please install FlashInfer.


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m Max pending requests is set to 141[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m Downloading model and tokenizer.[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(DetokenizeUDF)) pid=34476, ip=10.0.67.56)[0m No cloud storage mirror configured[32m [repeated 6x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m You have video processor config saved in `preprocessor.json` file which is deprecated. Video processor configs should be saved in their own `video_preprocessor.json` file. You can rename the file or load and save the processor back which renames it automatically. Loading from `preprocessor.json` will be removed in v5.0.
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m You have video processor config saved in `preprocessor.json` file which is deprecated. Video processor configs should be saved in their own `video_preprocessor.json` file. You can rename the file or load and save the processor back which renames it automatically. Loading from `preprocessor.json` will be removed in v5.0.


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [1;36m(Worker pid=41360)[0;0m INFO 11-13 00:46:48 [gpu_model_runner.py:2602] Starting to load model Qwen/Qwen2.5-VL-3B-Instruct...
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [1;36m(Worker pid=41360)[0;0m INFO 11-13 00:46:48 [gpu_model_runner.py:2634] Loading model from scratch...
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [1;36m(Worker pid=41360)[0;0m INFO 11-13 00:46:49 [cuda.py:366] Using Flash Attention backend on V1 engine.
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [1;36m(Worker pid=41360)[0;0m INFO 11-13 00:46:49 [weight_utils.py:392] Using model weights format ['*.safetensors']


    Loading safetensors checkpoint shards:   0% Completed | 0/2 [00:00<?, ?it/s]m [1;36m(Worker pid=41360)[0;0m 
    Loading safetensors checkpoint shards:  50% Completed | 1/2 [00:00<00:00,  2.22it/s]m(Worker pid=41360)[0;0m 


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m INFO 11-13 00:46:45 [shm_broadcast.py:289] vLLM message queue communication handle: Handle(local_reader_ranks=[0], buffer_handle=(1, 10485760, 10, 'psm_5d5e545d'), local_subscribe_addr='ipc:///tmp/469b1153-159d-42ba-94ee-07e7095202db', remote_subscribe_addr=None, remote_addr_ipv6=False)[32m [repeated 3x across cluster][0m


    Loading safetensors checkpoint shards: 100% Completed | 2/2 [00:01<00:00,  1.75it/s]m(Worker pid=41360)[0;0m 
    Loading safetensors checkpoint shards: 100% Completed | 2/2 [00:01<00:00,  1.81it/s]m(Worker pid=41360)[0;0m 
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [1;36m(Worker pid=41360)[0;0m 


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [1;36m(Worker pid=41360)[0;0m INFO 11-13 00:46:50 [default_loader.py:267] Loading weights took 1.24 seconds
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [1;36m(Worker pid=41360)[0;0m INFO 11-13 00:46:51 [gpu_model_runner.py:2653] Model loading took 7.1550 GiB and 1.776413 seconds
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m [Gloo] Rank 0 is connected to 0 peer ranks. Expected number of connected peer ranks is : 0[32m [repeated 18x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m INFO 11-13 00:46:46 [parallel_state.py:1208] rank 0 in world size 1 is assigned as DP rank 0, PP rank 0, TP rank 0, EP rank 0[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [1;36m(Worker pid=41360)[0;0m INFO 11-13 00:46:51 [gpu_model_runner.py:3344] Encoder cache will be initialized with a budget of 16384 tokens, and profiled with 1 image items of the maximum feature size.
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m WARNING 11-13 00:46:46 [topk_topp_sampler.py:66] FlashInfer is not available. Falling back to the PyTorch-native implementation of top-p & top-k sampling. For the best performance, please install FlashInfer.[32m [repeated 3x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m [1;36m(Worker pid=35966)[0;0m 
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m You have video processor config saved in `preprocessor.json` file which is deprecated. Video processor configs should be saved in their own `video_preprocessor.json` file. You can rename the file or load and save the processor back which renames it automatically. Loading from `preprocessor.json` will be removed in v5.0.[32m [repeated 2x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m [1;36m(Worker pid=41568)[0;0m 


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m [1;36m(Worker pid=41568)[0;0m INFO 11-13 00:46:49 [gpu_model_runner.py:2602] Starting to load model Qwen/Qwen2.5-VL-3B-Instruct...[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m [1;36m(Worker pid=41568)[0;0m INFO 11-13 00:46:49 [gpu_model_runner.py:2634] Loading model from scratch...[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m [1;36m(Worker pid=41568)[0;0m INFO 11-13 00:46:49 [cuda.py:366] Using Flash Attention backend on V1 engine.[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m [1;36m(Worker pid=41568)[0;0m INFO 11-13 00:46:50 [weight_utils.py:392] Using model weights format ['*.safetensors'][32m [repeated 3x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m [1;36m(Worker pid=37014)[0;0m 
    Loading safetensors checkpoint shards:   0% Completed | 0/2 [00:00<?, ?it/s][32m [repeated 3x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [1;36m(Worker pid=41360)[0;0m INFO 11-13 00:47:04 [backends.py:548] Using cache directory: /home/ray/.cache/vllm/torch_compile_cache/a314dc5b3a/rank_0_0/backbone for vLLM's torch.compile
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [1;36m(Worker pid=41360)[0;0m INFO 11-13 00:47:04 [backends.py:559] Dynamo bytecode transform time: 6.23 s
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m [1;36m(Worker pid=37014)[0;0m INFO 11-13 00:46:54 [default_loader.py:267] Loading weights took 4.89 seconds[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m [1;36m(Worker pid=37014)[0;0m INFO 11-13 00:46:55 [gpu_model_runner.py:2653] Model loading took 7.1550 GiB and 5.422226 seconds[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m [1;36m(Worker pid=37014)[0;0m INFO 11-13 00:46:55 [gpu_model_runner.py:3344] Encoder cache will be initialized with a budget of 16384 tokens, and profiled with 1 image items of the maximum feature size.[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [1;36m(Worker pid=41360)[0;0m INFO 11-13 00:47:06 [backends.py:164] Directly load the compiled graph(s) for dynamic shape from the cache, took 1.654 s
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [1;36m(Worker pid=41360)[0;0m INFO 11-13 00:47:07 [monitor.py:34] torch.compile takes 6.23 s in total
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [1;36m(Worker pid=41360)[0;0m INFO 11-13 00:47:08 [gpu_worker.py:298] Available KV cache memory: 9.26 GiB
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [1;36m(EngineCore_DP0 pid=41340)[0;0m INFO 11-13 00:47:08 [kv_cache_utils.py:1087] GPU KV cache size: 269,568 tokens
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [1;36m(EngineCore_DP0 pid=41340)[0;0m INFO 11-13 00:47:08 [kv_cache_utils.py:1091] Maximum concurrency for 8,192 tokens per request: 32.91x


    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):   0%|          | 0/35 [00:00<?, ?it/s]d=41360)[0;0m 
    Loading safetensors checkpoint shards: 100% Completed | 2/2 [00:04<00:00,  2.38s/it][32m [repeated 9x across cluster][0m
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):   6%|▌         | 2/35 [00:00<00:02, 13.73it/s]
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  11%|█▏        | 4/35 [00:00<00:02, 14.30it/s]
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  17%|█▋        | 6/35 [00:00<00:02, 14.30it/s]
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  23%|██▎       | 8/35 [00:00<00:01, 14.62it/s]
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  29%|██▊       | 10/35 [00:00<00:01, 14.92it/s]
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  34%|███▍      | 12/35 [00:00<00:01, 15.12it/s]
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  40%|████      | 14/35 [00:00<00:01, 14.39it/s]
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  46%|████▌     | 16/35 [00:01<00:01, 14.58it/s]


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m [1;36m(Worker pid=37014)[0;0m INFO 11-13 00:47:08 [backends.py:548] Using cache directory: /home/ray/.cache/vllm/torch_compile_cache/a314dc5b3a/rank_0_0/backbone for vLLM's torch.compile[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m [1;36m(Worker pid=37014)[0;0m INFO 11-13 00:47:08 [backends.py:559] Dynamo bytecode transform time: 6.45 s[32m [repeated 3x across cluster][0m


    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  91%|█████████▏| 32/35 [00:02<00:00, 15.95it/s]
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  97%|█████████▋| 34/35 [00:02<00:00, 16.07it/s]
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE): 100%|██████████| 35/35 [00:02<00:00, 15.10it/s]
    Capturing CUDA graphs (decode, FULL):   0%|          | 0/19 [00:00<?, ?it/s]m [1;36m(Worker pid=41360)[0;0m 
    Capturing CUDA graphs (decode, FULL):  11%|█         | 2/19 [00:00<00:01, 14.71it/s]
    Capturing CUDA graphs (decode, FULL):  21%|██        | 4/19 [00:00<00:01, 14.85it/s]
    Capturing CUDA graphs (decode, FULL):  32%|███▏      | 6/19 [00:00<00:00, 15.18it/s]
    Capturing CUDA graphs (decode, FULL):  42%|████▏     | 8/19 [00:00<00:00, 15.39it/s]
    Capturing CUDA graphs (decode, FULL):  53%|█████▎    | 10/19 [00:00<00:00, 16.05it/s]
    Capturing CUDA graphs (decode, FULL):  63%|██████▎   | 12/19 [00:00<00:00, 16.42it/s]


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m [1;36m(Worker pid=37014)[0;0m INFO 11-13 00:47:10 [backends.py:164] Directly load the compiled graph(s) for dynamic shape from the cache, took 1.693 s[32m [repeated 3x across cluster][0m


    Capturing CUDA graphs (decode, FULL):  74%|███████▎  | 14/19 [00:00<00:00, 16.62it/s]
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=38823, ip=10.0.70.78)[0m Failed to convert column '__data' into pyarrow array due to: Error converting data to Arrow: [{'caption.txt': 'Melanie Klein', 'image.png': {'bytes': b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xff\xdb\x00C\x00\x08\x06\x06\x07\x06\x05\x08\x07\x07\x07\t\t\x08\n\x0c\x...; falling back to serialize as pickled python objects
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=38823, ip=10.0.70.78)[0m Traceback (most recent call last):
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=38823, ip=10.0.70.78)[0m   File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/air/util/tensor_extensions/arrow.py", line 228, in _convert_to_pyarrow_native_array
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=38823, ip=10.0.70.78)[0m     pa_type = _infer_pyarrow_type(column_values)
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=38823, ip=10.0.70.78)[0m               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=38823, ip=10.0.70.78)[0m   File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/air/util/tensor_extensions/arrow.py", line 341, in _infer_pyarrow_type
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=38823, ip=10.0.70.78)[0m     inferred_pa_dtype = pa.infer_type(column_values)
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=38823, ip=10.0.70.78)[0m                         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=38823, ip=10.0.70.78)[0m   File "pyarrow/array.pxi", line 571, in pyarrow.lib.infer_type
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=38823, ip=10.0.70.78)[0m   File "pyarrow/error.pxi", line 155, in pyarrow.lib.pyarrow_internal_check_status
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=38823, ip=10.0.70.78)[0m   File "pyarrow/error.pxi", line 92, in pyarrow.lib.check_status
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=38823, ip=10.0.70.78)[0m pyarrow.lib.ArrowInvalid: Could not convert <PIL.Image.Image image mode=RGB size=225x225 at 0x7484F284E6D0> with type Image: did not recognize Python value type when inferring an Arrow data type
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=38823, ip=10.0.70.78)[0m 
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=38823, ip=10.0.70.78)[0m The above exception was the direct cause of the following exception:
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=38823, ip=10.0.70.78)[0m 
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=38823, ip=10.0.70.78)[0m Traceback (most recent call last):
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=38823, ip=10.0.70.78)[0m   File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/air/util/tensor_extensions/arrow.py", line 145, in convert_to_pyarrow_array
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=38823, ip=10.0.70.78)[0m     return _convert_to_pyarrow_native_array(column_values, column_name)
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=38823, ip=10.0.70.78)[0m            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=38823, ip=10.0.70.78)[0m   File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/air/util/tensor_extensions/arrow.py", line 255, in _convert_to_pyarrow_native_array
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=38823, ip=10.0.70.78)[0m     raise ArrowConversionError(str(column_values)) from e
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=38823, ip=10.0.70.78)[0m ray.air.util.tensor_extensions.arrow.ArrowConversionError: Error converting data to Arrow: [{'caption.txt': 'Melanie Klein', 'image.png': {'bytes': b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xff\xdb\x00C\x00\x08\x06\x06\x07\x06\x05\x08\x07\x07\x07\t\t\x08\n\x0c\x...
    Capturing CUDA graphs (decode, FULL):  84%|████████▍ | 16/19 [00:00<00:00, 16.85it/s]
    Capturing CUDA graphs (decode, FULL):  95%|█████████▍| 18/19 [00:01<00:00, 16.78it/s]
    Capturing CUDA graphs (decode, FULL): 100%|██████████| 19/19 [00:01<00:00, 16.29it/s]
    [36m(MapWorker(MapBatches(ChatTemplateUDF)) pid=32158, ip=10.0.67.56)[0m 
    [36m(MapWorker(MapBatches(ChatTemplateUDF)) pid=32158, ip=10.0.67.56)[0m 
    [36m(MapWorker(MapBatches(TokenizeUDF)) pid=38543, ip=10.0.110.182)[0m 
    [36m(MapWorker(MapBatches(TokenizeUDF)) pid=38543, ip=10.0.110.182)[0m 


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m [1;36m(Worker pid=37014)[0;0m INFO 11-13 00:47:11 [monitor.py:34] torch.compile takes 6.45 s in total[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [1;36m(Worker pid=41360)[0;0m INFO 11-13 00:47:12 [gpu_model_runner.py:3480] Graph capturing finished in 4 secs, took 0.44 GiB
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [1;36m(EngineCore_DP0 pid=41340)[0;0m INFO 11-13 00:47:12 [core.py:210] init engine (profile, create kv cache, warmup model) took 21.28 seconds


    Capturing CUDA graphs (decode, FULL): 100%|██████████| 19/19 [00:01<00:00, 17.43it/s]


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m [1;36m(Worker pid=37014)[0;0m INFO 11-13 00:47:12 [gpu_worker.py:298] Available KV cache memory: 9.26 GiB[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m [1;36m(EngineCore_DP0 pid=36992)[0;0m INFO 11-13 00:47:12 [kv_cache_utils.py:1087] GPU KV cache size: 269,568 tokens[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m [1;36m(EngineCore_DP0 pid=36992)[0;0m INFO 11-13 00:47:12 [kv_cache_utils.py:1091] Maximum concurrency for 8,192 tokens per request: 32.91x[32m [repeated 3x across cluster][0m


    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):   0%|          | 0/35 [00:00<?, ?it/s][32m [repeated 3x across cluster][0m
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  40%|████      | 14/35 [00:00<00:01, 14.45it/s][32m [repeated 40x across cluster][0m
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE): 100%|██████████| 35/35 [00:02<00:00, 15.00it/s][32m [repeated 9x across cluster][0m
    Capturing CUDA graphs (decode, FULL):   0%|          | 0/19 [00:00<?, ?it/s][32m [repeated 3x across cluster][0m
    Capturing CUDA graphs (decode, FULL):  74%|███████▎  | 14/19 [00:00<00:00, 17.85it/s][32m [repeated 23x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [1;36m(EngineCore_DP0 pid=41340)[0;0m You have video processor config saved in `preprocessor.json` file which is deprecated. Video processor configs should be saved in their own `video_preprocessor.json` file. You can rename the file or load and save the processor back which renames it automatically. Loading from `preprocessor.json` will be removed in v5.0.
    [36m(MapWorker(MapBatches(TokenizeUDF)) pid=38543, ip=10.0.110.182)[0m Failed to convert column '__data' into pyarrow array due to: Error converting data to Arrow: [{'caption.txt': 'Melanie Klein', 'image.png': {'bytes': b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xff\xdb\x00C\x00\x08\x06\x06\x07\x06\x05\x08\x07\x07\x07\t\t\x08\n\x0c\x...; falling back to serialize as pickled python objects[32m [repeated 2x across cluster][0m
    [36m(MapWorker(MapBatches(TokenizeUDF)) pid=38543, ip=10.0.110.182)[0m Traceback (most recent call last):[32m [repeated 4x across cluster][0m
    [36m(MapWorker(MapBatches(TokenizeUDF)) pid=38543, ip=10.0.110.182)[0m   File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/air/util/tensor_extensions/arrow.py", line 255, in _convert_to_pyarrow_native_array[32m [repeated 4x across cluster][0m
    [36m(MapWorker(MapBatches(TokenizeUDF)) pid=38543, ip=10.0.110.182)[0m     pa_type = _infer_pyarrow_type(column_values)[32m [repeated 2x across cluster][0m
    [36m(MapWorker(MapBatches(TokenizeUDF)) pid=38543, ip=10.0.110.182)[0m               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^[32m [repeated 2x across cluster][0m
    [36m(MapWorker(MapBatches(TokenizeUDF)) pid=38543, ip=10.0.110.182)[0m   File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/air/util/tensor_extensions/arrow.py", line 341, in _infer_pyarrow_type[32m [repeated 2x across cluster][0m
    [36m(MapWorker(MapBatches(TokenizeUDF)) pid=38543, ip=10.0.110.182)[0m     inferred_pa_dtype = pa.infer_type(column_values)[32m [repeated 2x across cluster][0m
    [36m(MapWorker(MapBatches(TokenizeUDF)) pid=38543, ip=10.0.110.182)[0m                         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^[32m [repeated 2x across cluster][0m
    [36m(MapWorker(MapBatches(TokenizeUDF)) pid=38543, ip=10.0.110.182)[0m   File "pyarrow/array.pxi", line 571, in pyarrow.lib.infer_type[32m [repeated 2x across cluster][0m
    [36m(MapWorker(MapBatches(TokenizeUDF)) pid=38543, ip=10.0.110.182)[0m   File "pyarrow/error.pxi", line 155, in pyarrow.lib.pyarrow_internal_check_status[32m [repeated 2x across cluster][0m
    [36m(MapWorker(MapBatches(TokenizeUDF)) pid=38543, ip=10.0.110.182)[0m   File "pyarrow/error.pxi", line 92, in pyarrow.lib.check_status[32m [repeated 2x across cluster][0m
    [36m(MapWorker(MapBatches(TokenizeUDF)) pid=38543, ip=10.0.110.182)[0m pyarrow.lib.ArrowInvalid: Could not convert <PIL.Image.Image image mode=RGB size=225x225 at 0x7309AA137A50> with type Image: did not recognize Python value type when inferring an Arrow data type[32m [repeated 2x across cluster][0m
    [36m(MapWorker(MapBatches(TokenizeUDF)) pid=38543, ip=10.0.110.182)[0m The above exception was the direct cause of the following exception:[32m [repeated 2x across cluster][0m
    [36m(MapWorker(MapBatches(TokenizeUDF)) pid=38543, ip=10.0.110.182)[0m   File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/air/util/tensor_extensions/arrow.py", line 145, in convert_to_pyarrow_array[32m [repeated 2x across cluster][0m
    [36m(MapWorker(MapBatches(TokenizeUDF)) pid=38543, ip=10.0.110.182)[0m     return _convert_to_pyarrow_native_array(column_values, column_name)[32m [repeated 2x across cluster][0m
    [36m(MapWorker(MapBatches(TokenizeUDF)) pid=38543, ip=10.0.110.182)[0m            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^[32m [repeated 2x across cluster][0m
    [36m(MapWorker(MapBatches(TokenizeUDF)) pid=38543, ip=10.0.110.182)[0m     raise ArrowConversionError(str(column_values)) from e[32m [repeated 2x across cluster][0m
    [36m(MapWorker(MapBatches(TokenizeUDF)) pid=38543, ip=10.0.110.182)[0m ray.air.util.tensor_extensions.arrow.ArrowConversionError: Error converting data to Arrow: [{'caption.txt': 'Melanie Klein', 'image.png': {'bytes': b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xff\xdb\x00C\x00\x08\x06\x06\x07\x06\x05\x08\x07\x07\x07\t\t\x08\n\x0c\x...[32m [repeated 2x across cluster][0m
    Capturing CUDA graphs (decode, FULL): 100%|██████████| 19/19 [00:01<00:00, 16.66it/s][32m [repeated 2x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m INFO 11-13 00:47:18 [loggers.py:147] Engine 000: vllm cache_config_info with initialization after num_gpu_blocks is: 16848
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m [1;36m(Worker pid=37014)[0;0m INFO 11-13 00:47:16 [gpu_model_runner.py:3480] Graph capturing finished in 4 secs, took 0.44 GiB[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m [1;36m(EngineCore_DP0 pid=36992)[0;0m INFO 11-13 00:47:16 [core.py:210] init engine (profile, create kv cache, warmup model) took 21.53 seconds[32m [repeated 3x across cluster][0m


    Capturing CUDA graphs (decode, FULL): 100%|██████████| 19/19 [00:01<00:00, 17.72it/s]
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  86%|████████▌ | 30/35 [00:02<00:00, 15.64it/s][32m [repeated 12x across cluster][0m
    Capturing CUDA graphs (decode, FULL):  84%|████████▍ | 16/19 [00:00<00:00, 17.94it/s]
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m You have video processor config saved in `preprocessor.json` file which is deprecated. Video processor configs should be saved in their own `video_preprocessor.json` file. You can rename the file or load and save the processor back which renames it automatically. Loading from `preprocessor.json` will be removed in v5.0.[32m [repeated 6x across cluster][0m
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=36152, ip=10.0.97.54)[0m Failed to convert column '__data' into pyarrow array due to: Error converting data to Arrow: [{'caption.txt': 'Marine Knots & Splices class', 'image.png': {'bytes': b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xff\xdb\x00C\x00\x08\x06\x06\x07\x06\x05\x08\x07\x07\x07\...; falling back to serialize as pickled python objects
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=36152, ip=10.0.97.54)[0m Traceback (most recent call last):
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=36152, ip=10.0.97.54)[0m   File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/air/util/tensor_extensions/arrow.py", line 228, in _convert_to_pyarrow_native_array
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=36152, ip=10.0.97.54)[0m     pa_type = _infer_pyarrow_type(column_values)
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=36152, ip=10.0.97.54)[0m               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=36152, ip=10.0.97.54)[0m   File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/air/util/tensor_extensions/arrow.py", line 341, in _infer_pyarrow_type
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=36152, ip=10.0.97.54)[0m     inferred_pa_dtype = pa.infer_type(column_values)
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=36152, ip=10.0.97.54)[0m                         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=36152, ip=10.0.97.54)[0m   File "pyarrow/array.pxi", line 571, in pyarrow.lib.infer_type
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=36152, ip=10.0.97.54)[0m   File "pyarrow/error.pxi", line 155, in pyarrow.lib.pyarrow_internal_check_status
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=36152, ip=10.0.97.54)[0m   File "pyarrow/error.pxi", line 92, in pyarrow.lib.check_status
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=36152, ip=10.0.97.54)[0m pyarrow.lib.ArrowInvalid: Could not convert <PIL.Image.Image image mode=RGB size=225x225 at 0x7AC040A9A5D0> with type Image: did not recognize Python value type when inferring an Arrow data type
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=36152, ip=10.0.97.54)[0m 
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=36152, ip=10.0.97.54)[0m The above exception was the direct cause of the following exception:
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=36152, ip=10.0.97.54)[0m 
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=36152, ip=10.0.97.54)[0m Traceback (most recent call last):
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=36152, ip=10.0.97.54)[0m   File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/air/util/tensor_extensions/arrow.py", line 145, in convert_to_pyarrow_array
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=36152, ip=10.0.97.54)[0m     return _convert_to_pyarrow_native_array(column_values, column_name)
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=36152, ip=10.0.97.54)[0m            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=36152, ip=10.0.97.54)[0m   File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/air/util/tensor_extensions/arrow.py", line 255, in _convert_to_pyarrow_native_array
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=36152, ip=10.0.97.54)[0m     raise ArrowConversionError(str(column_values)) from e
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=36152, ip=10.0.97.54)[0m ray.air.util.tensor_extensions.arrow.ArrowConversionError: Error converting data to Arrow: [{'caption.txt': 'Marine Knots & Splices class', 'image.png': {'bytes': b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xff\xdb\x00C\x00\x08\x06\x06\x07\x06\x05\x08\x07\x07\x07\...
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=42184, ip=10.0.97.152)[0m Failed to convert column '__data' into pyarrow array due to: Error converting data to Arrow: [{'caption.txt': 'tri fold invitation template', 'image.png': {'bytes': b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xff\xdb\x00C\x00\x08\x06\x06\x07\x06\x05\x08\x07\x07\x07\...; falling back to serialize as pickled python objects
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=42184, ip=10.0.97.152)[0m 
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=42184, ip=10.0.97.152)[0m 
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=42184, ip=10.0.97.152)[0m ray.air.util.tensor_extensions.arrow.ArrowConversionError: Error converting data to Arrow: [{'caption.txt': 'tri fold invitation template', 'image.png': {'bytes': b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xff\xdb\x00C\x00\x08\x06\x06\x07\x06\x05\x08\x07\x07\x07\...
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m You have video processor config saved in `preprocessor.json` file which is deprecated. Video processor configs should be saved in their own `video_preprocessor.json` file. You can rename the file or load and save the processor back which renames it automatically. Loading from `preprocessor.json` will be removed in v5.0.
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=41775, ip=10.0.107.216)[0m Failed to convert column '__data' into pyarrow array due to: Error converting data to Arrow: [{'caption.txt': 'Bausch &amp; Lomb Sight Savers Contact Lens Cases 3 Each (Pack of 4)', 'image.png': {'bytes': b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xff\xdb\x00C\x00\...; falling back to serialize as pickled python objects
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=41775, ip=10.0.107.216)[0m 
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=41775, ip=10.0.107.216)[0m 
    [36m(MapWorker(Map(_preprocess)->MapBatches(PrepareImageUDF)) pid=41775, ip=10.0.107.216)[0m ray.air.util.tensor_extensions.arrow.ArrowConversionError: Error converting data to Arrow: [{'caption.txt': 'Bausch &amp; Lomb Sight Savers Contact Lens Cases 3 Each (Pack of 4)', 'image.png': {'bytes': b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xff\xdb\x00C\x00\...


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m INFO 11-13 00:47:35 [loggers.py:127] Engine 000: Avg prompt throughput: 1496.5 tokens/s, Avg generation throughput: 1210.0 tokens/s, Running: 126 reqs, Waiting: 12 reqs, GPU KV cache usage: 7.0%, Prefix cache hit rate: 29.0%
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m INFO 11-13 00:47:22 [loggers.py:147] Engine 000: vllm cache_config_info with initialization after num_gpu_blocks is: 16848[32m [repeated 3x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m [vLLM] Elapsed time for batch 167d16127b5d4fb7be249b3cb57152bb with size 16: 13.422863879000943
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m Failed to convert column '__data' into pyarrow array due to: Error converting data to Arrow: [{'caption.txt': '2000 Land Rover Discovery', 'image.png': {'bytes': b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xff\xdb\x00C\x00\x08\x06\x06\x07\x06\x05\x08\x07\x07\x07\t\t...; falling back to serialize as pickled python objects
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m 
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m 
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m ray.air.util.tensor_extensions.arrow.ArrowConversionError: Error converting data to Arrow: [{'caption.txt': '2000 Land Rover Discovery', 'image.png': {'bytes': b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xff\xdb\x00C\x00\x08\x06\x06\x07\x06\x05\x08\x07\x07\x07\t\t...
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m Traceback (most recent call last):[32m [repeated 6x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m   File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/air/util/tensor_extensions/arrow.py", line 255, in _convert_to_pyarrow_native_array[32m [repeated 6x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m     pa_type = _infer_pyarrow_type(column_values)[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m   File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/air/util/tensor_extensions/arrow.py", line 341, in _infer_pyarrow_type[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m     inferred_pa_dtype = pa.infer_type(column_values)[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m                         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m   File "pyarrow/array.pxi", line 571, in pyarrow.lib.infer_type[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m   File "pyarrow/error.pxi", line 155, in pyarrow.lib.pyarrow_internal_check_status[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m   File "pyarrow/error.pxi", line 92, in pyarrow.lib.check_status[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m pyarrow.lib.ArrowInvalid: Could not convert <PIL.Image.Image image mode=RGB size=225x225 at 0x7F2DF71AFCD0> with type Image: did not recognize Python value type when inferring an Arrow data type[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m The above exception was the direct cause of the following exception:[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m   File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/air/util/tensor_extensions/arrow.py", line 145, in convert_to_pyarrow_array[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m     return _convert_to_pyarrow_native_array(column_values, column_name)[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m     raise ArrowConversionError(str(column_values)) from e[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m Failed to convert column '__data' into pyarrow array due to: Error converting data to Arrow: [{'caption.txt': "Batman's looking suave in Arkham Knight! ", 'image.png': {'bytes': b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xff\xdb\x00C\x00\x08\x06\x06\x07\x06\x05\x08...; falling back to serialize as pickled python objects
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m 
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m 
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m ray.air.util.tensor_extensions.arrow.ArrowConversionError: Error converting data to Arrow: [{'caption.txt': "Batman's looking suave in Arkham Knight! ", 'image.png': {'bytes': b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xff\xdb\x00C\x00\x08\x06\x06\x07\x06\x05\x08...
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [vLLM] Elapsed time for batch 8cf1c61d6fb14bcea4b2b8cb04af5c38 with size 16: 19.0157169769991[32m [repeated 6x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m Traceback (most recent call last):[32m [repeated 2x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m   File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/air/util/tensor_extensions/arrow.py", line 255, in _convert_to_pyarrow_native_array[32m [repeated 2x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m     pa_type = _infer_pyarrow_type(column_values)
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m   File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/air/util/tensor_extensions/arrow.py", line 341, in _infer_pyarrow_type
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m     inferred_pa_dtype = pa.infer_type(column_values)
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m                         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m   File "pyarrow/array.pxi", line 571, in pyarrow.lib.infer_type
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m   File "pyarrow/error.pxi", line 155, in pyarrow.lib.pyarrow_internal_check_status
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m   File "pyarrow/error.pxi", line 92, in pyarrow.lib.check_status
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m pyarrow.lib.ArrowInvalid: Could not convert <PIL.Image.Image image mode=RGB size=225x225 at 0x744A78DBE150> with type Image: did not recognize Python value type when inferring an Arrow data type
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m The above exception was the direct cause of the following exception:
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m   File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/air/util/tensor_extensions/arrow.py", line 145, in convert_to_pyarrow_array
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m     return _convert_to_pyarrow_native_array(column_values, column_name)
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m     raise ArrowConversionError(str(column_values)) from e


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m INFO 11-13 00:47:40 [loggers.py:127] Engine 000: Avg prompt throughput: 1387.2 tokens/s, Avg generation throughput: 1562.1 tokens/s, Running: 127 reqs, Waiting: 4 reqs, GPU KV cache usage: 7.9%, Prefix cache hit rate: 29.0%[32m [repeated 6x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m Failed to convert column '__data' into pyarrow array due to: Error converting data to Arrow: [{'caption.txt': '"Cafe Van goch" stock image', 'image.png': {'bytes': b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xff\xdb\x00C\x00\x08\x06\x06\x07\x06\x05\x08\x07\x07\x07\t...; falling back to serialize as pickled python objects
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m     pa_type = _infer_pyarrow_type(column_values)
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m   File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/air/util/tensor_extensions/arrow.py", line 341, in _infer_pyarrow_type
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m     inferred_pa_dtype = pa.infer_type(column_values)
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m                         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m   File "pyarrow/array.pxi", line 571, in pyarrow.lib.infer_type
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m   File "pyarrow/error.pxi", line 155, in pyarrow.lib.pyarrow_internal_check_status
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m   File "pyarrow/error.pxi", line 92, in pyarrow.lib.check_status
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m pyarrow.lib.ArrowInvalid: Could not convert <PIL.Image.Image image mode=RGB size=225x225 at 0x7B8CB42BB790> with type Image: did not recognize Python value type when inferring an Arrow data type
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m 
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m The above exception was the direct cause of the following exception:
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m 
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m   File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/air/util/tensor_extensions/arrow.py", line 145, in convert_to_pyarrow_array
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m     return _convert_to_pyarrow_native_array(column_values, column_name)
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m     raise ArrowConversionError(str(column_values)) from e
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m ray.air.util.tensor_extensions.arrow.ArrowConversionError: Error converting data to Arrow: [{'caption.txt': '"Cafe Van goch" stock image', 'image.png': {'bytes': b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xff\xdb\x00C\x00\x08\x06\x06\x07\x06\x05\x08\x07\x07\x07\t...
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m Failed to convert column '__data' into pyarrow array due to: Error converting data to Arrow: [{'caption.txt': 'Killing Floor 2: Digital Deluxe Edition (2016) (RePack от FitGirl) PC', 'image.png': {'bytes': b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xff\xdb\x00C\x00...; falling back to serialize as pickled python objects
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m 
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m 
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m ray.air.util.tensor_extensions.arrow.ArrowConversionError: Error converting data to Arrow: [{'caption.txt': 'Killing Floor 2: Digital Deluxe Edition (2016) (RePack от FitGirl) PC', 'image.png': {'bytes': b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xff\xdb\x00C\x00...
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m [vLLM] Elapsed time for batch 343cf008ccab4f00aea4cd8c0a8678b7 with size 16: 20.54477966400009[32m [repeated 25x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m Traceback (most recent call last):[32m [repeated 4x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m   File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/air/util/tensor_extensions/arrow.py", line 255, in _convert_to_pyarrow_native_array[32m [repeated 4x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m INFO 11-13 00:47:47 [loggers.py:127] Engine 000: Avg prompt throughput: 1249.6 tokens/s, Avg generation throughput: 1750.9 tokens/s, Running: 127 reqs, Waiting: 12 reqs, GPU KV cache usage: 7.8%, Prefix cache hit rate: 29.0%[32m [repeated 24x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m     pa_type = _infer_pyarrow_type(column_values)
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m   File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/air/util/tensor_extensions/arrow.py", line 341, in _infer_pyarrow_type
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m     inferred_pa_dtype = pa.infer_type(column_values)
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m                         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m   File "pyarrow/array.pxi", line 571, in pyarrow.lib.infer_type
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m   File "pyarrow/error.pxi", line 155, in pyarrow.lib.pyarrow_internal_check_status
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m   File "pyarrow/error.pxi", line 92, in pyarrow.lib.check_status
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m pyarrow.lib.ArrowInvalid: Could not convert <PIL.Image.Image image mode=RGB size=225x225 at 0x7191DD721890> with type Image: did not recognize Python value type when inferring an Arrow data type
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m The above exception was the direct cause of the following exception:
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m   File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/air/util/tensor_extensions/arrow.py", line 145, in convert_to_pyarrow_array
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m     return _convert_to_pyarrow_native_array(column_values, column_name)
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m     raise ArrowConversionError(str(column_values)) from e
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m [vLLM] Elapsed time for batch ab38e47be9214c1d8a2449c2d790afe5 with size 16: 25.8210009430004[32m [repeated 9x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m INFO 11-13 00:47:52 [loggers.py:127] Engine 000: Avg prompt throughput: 1126.1 tokens/s, Avg generation throughput: 1747.2 tokens/s, Running: 127 reqs, Waiting: 12 reqs, GPU KV cache usage: 8.0%, Prefix cache hit rate: 29.3%[32m [repeated 14x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m [vLLM] Elapsed time for batch 2e046b4eb0ba4881ad632787413c77b9 with size 16: 33.7588008670009[32m [repeated 18x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m INFO 11-13 00:47:57 [loggers.py:127] Engine 000: Avg prompt throughput: 1045.1 tokens/s, Avg generation throughput: 1853.2 tokens/s, Running: 127 reqs, Waiting: 13 reqs, GPU KV cache usage: 7.7%, Prefix cache hit rate: 29.4%[32m [repeated 14x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [vLLM] Elapsed time for batch 2b844038b728452892645b240ec4554b with size 16: 22.92775692000032[32m [repeated 11x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m INFO 11-13 00:48:02 [loggers.py:127] Engine 000: Avg prompt throughput: 1433.0 tokens/s, Avg generation throughput: 1667.5 tokens/s, Running: 126 reqs, Waiting: 13 reqs, GPU KV cache usage: 7.5%, Prefix cache hit rate: 29.1%[32m [repeated 14x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m [vLLM] Elapsed time for batch 84046a1fc8284d3586df5a07b7bcc3ca with size 16: 21.607344562000435[32m [repeated 20x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m INFO 11-13 00:48:07 [loggers.py:127] Engine 000: Avg prompt throughput: 1250.7 tokens/s, Avg generation throughput: 1697.9 tokens/s, Running: 126 reqs, Waiting: 0 reqs, GPU KV cache usage: 7.7%, Prefix cache hit rate: 29.3%[32m [repeated 21x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [vLLM] Elapsed time for batch cb46f8e0e4c940fdb0cd2318a665f900 with size 16: 19.56577010699948[32m [repeated 20x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m INFO 11-13 00:48:12 [loggers.py:127] Engine 000: Avg prompt throughput: 0.0 tokens/s, Avg generation throughput: 1385.6 tokens/s, Running: 44 reqs, Waiting: 0 reqs, GPU KV cache usage: 2.8%, Prefix cache hit rate: 29.0%[32m [repeated 24x across cluster][0m


    [36m(MapWorker(MapBatches(DetokenizeUDF)) pid=34476, ip=10.0.67.56)[0m Failed to convert column '__data' into pyarrow array due to: Error converting data to Arrow: [{'caption.txt': 'Melanie Klein', 'image.png': {'bytes': b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xff\xdb\x00C\x00\x08\x06\x06\x07\x06\x05\x08\x07\x07\x07\t\t\x08\n\x0c\x...; falling back to serialize as pickled python objects
    [36m(MapWorker(MapBatches(DetokenizeUDF)) pid=34476, ip=10.0.67.56)[0m Traceback (most recent call last):
    [36m(MapWorker(MapBatches(DetokenizeUDF)) pid=34476, ip=10.0.67.56)[0m   File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/air/util/tensor_extensions/arrow.py", line 228, in _convert_to_pyarrow_native_array
    [36m(MapWorker(MapBatches(DetokenizeUDF)) pid=34476, ip=10.0.67.56)[0m     pa_type = _infer_pyarrow_type(column_values)
    [36m(MapWorker(MapBatches(DetokenizeUDF)) pid=34476, ip=10.0.67.56)[0m               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    [36m(MapWorker(MapBatches(DetokenizeUDF)) pid=34476, ip=10.0.67.56)[0m   File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/air/util/tensor_extensions/arrow.py", line 341, in _infer_pyarrow_type
    [36m(MapWorker(MapBatches(DetokenizeUDF)) pid=34476, ip=10.0.67.56)[0m     inferred_pa_dtype = pa.infer_type(column_values)
    [36m(MapWorker(MapBatches(DetokenizeUDF)) pid=34476, ip=10.0.67.56)[0m                         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    [36m(MapWorker(MapBatches(DetokenizeUDF)) pid=34476, ip=10.0.67.56)[0m   File "pyarrow/array.pxi", line 571, in pyarrow.lib.infer_type
    [36m(MapWorker(MapBatches(DetokenizeUDF)) pid=34476, ip=10.0.67.56)[0m   File "pyarrow/error.pxi", line 155, in pyarrow.lib.pyarrow_internal_check_status
    [36m(MapWorker(MapBatches(DetokenizeUDF)) pid=34476, ip=10.0.67.56)[0m   File "pyarrow/error.pxi", line 92, in pyarrow.lib.check_status
    [36m(MapWorker(MapBatches(DetokenizeUDF)) pid=34476, ip=10.0.67.56)[0m pyarrow.lib.ArrowInvalid: Could not convert <PIL.Image.Image image mode=RGB size=225x225 at 0x7BAADBD56050> with type Image: did not recognize Python value type when inferring an Arrow data type
    [36m(MapWorker(MapBatches(DetokenizeUDF)) pid=34476, ip=10.0.67.56)[0m 
    [36m(MapWorker(MapBatches(DetokenizeUDF)) pid=34476, ip=10.0.67.56)[0m The above exception was the direct cause of the following exception:
    [36m(MapWorker(MapBatches(DetokenizeUDF)) pid=34476, ip=10.0.67.56)[0m 
    [36m(MapWorker(MapBatches(DetokenizeUDF)) pid=34476, ip=10.0.67.56)[0m Traceback (most recent call last):
    [36m(MapWorker(MapBatches(DetokenizeUDF)) pid=34476, ip=10.0.67.56)[0m   File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/air/util/tensor_extensions/arrow.py", line 145, in convert_to_pyarrow_array
    [36m(MapWorker(MapBatches(DetokenizeUDF)) pid=34476, ip=10.0.67.56)[0m     return _convert_to_pyarrow_native_array(column_values, column_name)
    [36m(MapWorker(MapBatches(DetokenizeUDF)) pid=34476, ip=10.0.67.56)[0m            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    [36m(MapWorker(MapBatches(DetokenizeUDF)) pid=34476, ip=10.0.67.56)[0m   File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/air/util/tensor_extensions/arrow.py", line 255, in _convert_to_pyarrow_native_array
    [36m(MapWorker(MapBatches(DetokenizeUDF)) pid=34476, ip=10.0.67.56)[0m     raise ArrowConversionError(str(column_values)) from e
    [36m(MapWorker(MapBatches(DetokenizeUDF)) pid=34476, ip=10.0.67.56)[0m ray.air.util.tensor_extensions.arrow.ArrowConversionError: Error converting data to Arrow: [{'caption.txt': 'Melanie Klein', 'image.png': {'bytes': b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xff\xdb\x00C\x00\x08\x06\x06\x07\x06\x05\x08\x07\x07\x07\t\t\x08\n\x0c\x...
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [vLLM] Elapsed time for batch 4183a83748db4c01a29f8e2c8389183b with size 12: 14.47526366999955[32m [repeated 28x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m INFO 11-13 00:48:18 [loggers.py:127] Engine 000: Avg prompt throughput: 0.0 tokens/s, Avg generation throughput: 673.6 tokens/s, Running: 18 reqs, Waiting: 0 reqs, GPU KV cache usage: 1.5%, Prefix cache hit rate: 29.4%[32m [repeated 21x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m [vLLM] Elapsed time for batch d09ec026591d4e53a1a00fc06e551ec9 with size 12: 13.430156858001283[32m [repeated 11x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m INFO 11-13 00:48:23 [loggers.py:127] Engine 000: Avg prompt throughput: 1943.8 tokens/s, Avg generation throughput: 1395.1 tokens/s, Running: 127 reqs, Waiting: 6 reqs, GPU KV cache usage: 7.1%, Prefix cache hit rate: 29.4%[32m [repeated 9x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [vLLM] Elapsed time for batch 7b9f4c5906654bbdb7e0a48d59a108f0 with size 16: 15.95833424400007[32m [repeated 9x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m INFO 11-13 00:48:29 [loggers.py:127] Engine 000: Avg prompt throughput: 1768.7 tokens/s, Avg generation throughput: 1439.9 tokens/s, Running: 126 reqs, Waiting: 11 reqs, GPU KV cache usage: 7.4%, Prefix cache hit rate: 29.1%[32m [repeated 8x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m INFO 11-13 00:48:34 [loggers.py:127] Engine 000: Avg prompt throughput: 1044.6 tokens/s, Avg generation throughput: 1657.6 tokens/s, Running: 127 reqs, Waiting: 12 reqs, GPU KV cache usage: 7.4%, Prefix cache hit rate: 29.3%[32m [repeated 11x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m [vLLM] Elapsed time for batch caaeef81bc2c4acaa2331d0075eeb7e4 with size 16: 17.53073360399867[32m [repeated 11x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m INFO 11-13 00:48:39 [loggers.py:127] Engine 000: Avg prompt throughput: 906.0 tokens/s, Avg generation throughput: 1968.0 tokens/s, Running: 127 reqs, Waiting: 13 reqs, GPU KV cache usage: 8.2%, Prefix cache hit rate: 29.4%[32m [repeated 19x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m [vLLM] Elapsed time for batch ced035fbbe0e4791b0347d2423269c08 with size 16: 23.221825806000197[32m [repeated 19x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m INFO 11-13 00:48:45 [loggers.py:127] Engine 000: Avg prompt throughput: 1409.5 tokens/s, Avg generation throughput: 1476.1 tokens/s, Running: 126 reqs, Waiting: 11 reqs, GPU KV cache usage: 7.4%, Prefix cache hit rate: 29.5%[32m [repeated 17x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m [vLLM] Elapsed time for batch 1b3cc4d9e629487983c82495b097a1c8 with size 16: 23.206531301000723[32m [repeated 17x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [vLLM] Elapsed time for batch 411b2ccd2bb641c4931eece50984bdcf with size 16: 24.08054963899849[32m [repeated 13x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m INFO 11-13 00:48:50 [loggers.py:127] Engine 000: Avg prompt throughput: 1352.6 tokens/s, Avg generation throughput: 1573.9 tokens/s, Running: 125 reqs, Waiting: 11 reqs, GPU KV cache usage: 7.4%, Prefix cache hit rate: 29.2%[32m [repeated 13x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [vLLM] Elapsed time for batch 5e001d266d684724967f8c91f1a77cdc with size 16: 23.83260104100009[32m [repeated 17x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m INFO 11-13 00:48:55 [loggers.py:127] Engine 000: Avg prompt throughput: 437.4 tokens/s, Avg generation throughput: 2461.6 tokens/s, Running: 114 reqs, Waiting: 0 reqs, GPU KV cache usage: 6.8%, Prefix cache hit rate: 29.1%[32m [repeated 17x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m INFO 11-13 00:49:00 [loggers.py:127] Engine 000: Avg prompt throughput: 897.6 tokens/s, Avg generation throughput: 2067.7 tokens/s, Running: 121 reqs, Waiting: 0 reqs, GPU KV cache usage: 7.3%, Prefix cache hit rate: 29.4%[32m [repeated 16x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m [vLLM] Elapsed time for batch e02a2ac786954f60a85b415355cc45cd with size 16: 20.324421184999665[32m [repeated 16x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m INFO 11-13 00:49:06 [loggers.py:127] Engine 000: Avg prompt throughput: 0.0 tokens/s, Avg generation throughput: 1654.8 tokens/s, Running: 55 reqs, Waiting: 0 reqs, GPU KV cache usage: 3.3%, Prefix cache hit rate: 29.5%[32m [repeated 28x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m [vLLM] Elapsed time for batch 479c63c3a6de487b97cf26fb9fb9ff6d with size 12: 14.195332227000108[32m [repeated 28x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m INFO 11-13 00:49:11 [loggers.py:127] Engine 000: Avg prompt throughput: 0.0 tokens/s, Avg generation throughput: 1310.2 tokens/s, Running: 36 reqs, Waiting: 0 reqs, GPU KV cache usage: 2.7%, Prefix cache hit rate: 29.4%[32m [repeated 21x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m [vLLM] Elapsed time for batch 32b946bc6d9042fdb3e643a4e616ed03 with size 12: 13.314241169000525[32m [repeated 21x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m INFO 11-13 00:49:16 [loggers.py:127] Engine 000: Avg prompt throughput: 0.0 tokens/s, Avg generation throughput: 2192.1 tokens/s, Running: 66 reqs, Waiting: 0 reqs, GPU KV cache usage: 4.8%, Prefix cache hit rate: 29.7%[32m [repeated 11x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m [vLLM] Elapsed time for batch 3ab62c2c341049ec901a217533e55f50 with size 12: 14.621904135001387[32m [repeated 11x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [vLLM] Elapsed time for batch e77a7e6b5d0645b091993fa77f801cdc with size 16: 14.839424179999696[32m [repeated 8x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m INFO 11-13 00:49:21 [loggers.py:127] Engine 000: Avg prompt throughput: 1687.9 tokens/s, Avg generation throughput: 1448.4 tokens/s, Running: 125 reqs, Waiting: 13 reqs, GPU KV cache usage: 7.5%, Prefix cache hit rate: 29.2%[32m [repeated 8x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m INFO 11-13 00:49:27 [loggers.py:127] Engine 000: Avg prompt throughput: 1404.6 tokens/s, Avg generation throughput: 1595.5 tokens/s, Running: 127 reqs, Waiting: 11 reqs, GPU KV cache usage: 7.4%, Prefix cache hit rate: 29.7%[32m [repeated 10x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m [vLLM] Elapsed time for batch 9b97f223f78e416db06feca24e0fb609 with size 16: 17.57454844700078[32m [repeated 10x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m INFO 11-13 00:49:32 [loggers.py:127] Engine 000: Avg prompt throughput: 2716.0 tokens/s, Avg generation throughput: 1018.3 tokens/s, Running: 127 reqs, Waiting: 6 reqs, GPU KV cache usage: 7.3%, Prefix cache hit rate: 29.5%[32m [repeated 18x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m [vLLM] Elapsed time for batch 2b759974a4394d3e8d59ca17468fc2c1 with size 16: 18.17719942400072[32m [repeated 18x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [vLLM] Elapsed time for batch 5b6e36ec8f644874b442fd04c5c78075 with size 16: 13.920583761000671[32m [repeated 16x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m INFO 11-13 00:49:38 [loggers.py:127] Engine 000: Avg prompt throughput: 1118.3 tokens/s, Avg generation throughput: 1561.6 tokens/s, Running: 126 reqs, Waiting: 13 reqs, GPU KV cache usage: 7.3%, Prefix cache hit rate: 29.3%[32m [repeated 16x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m INFO 11-13 00:49:43 [loggers.py:127] Engine 000: Avg prompt throughput: 885.5 tokens/s, Avg generation throughput: 1803.2 tokens/s, Running: 123 reqs, Waiting: 13 reqs, GPU KV cache usage: 7.4%, Prefix cache hit rate: 29.4%[32m [repeated 18x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m [vLLM] Elapsed time for batch 2637f9cec9ba499b9207d18d36bafcff with size 16: 23.155120430999887[32m [repeated 18x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [vLLM] Elapsed time for batch 53e8ccaa0f7944fdb476b5975a6eccf6 with size 16: 23.353506793000633[32m [repeated 14x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m INFO 11-13 00:49:49 [loggers.py:127] Engine 000: Avg prompt throughput: 1259.4 tokens/s, Avg generation throughput: 1637.9 tokens/s, Running: 126 reqs, Waiting: 11 reqs, GPU KV cache usage: 7.3%, Prefix cache hit rate: 29.3%[32m [repeated 14x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m INFO 11-13 00:49:54 [loggers.py:127] Engine 000: Avg prompt throughput: 1203.9 tokens/s, Avg generation throughput: 1672.0 tokens/s, Running: 126 reqs, Waiting: 12 reqs, GPU KV cache usage: 7.7%, Prefix cache hit rate: 29.3%[32m [repeated 19x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m [vLLM] Elapsed time for batch dbd85a5d444e408887239a3ca1030ab2 with size 16: 23.269112691999908[32m [repeated 19x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m INFO 11-13 00:49:59 [loggers.py:127] Engine 000: Avg prompt throughput: 1572.9 tokens/s, Avg generation throughput: 1204.7 tokens/s, Running: 60 reqs, Waiting: 0 reqs, GPU KV cache usage: 3.7%, Prefix cache hit rate: 29.5%[32m [repeated 24x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m [vLLM] Elapsed time for batch 9a7b85c4a1bd48e780fe227b08e2fde0 with size 16: 18.28119917799995[32m [repeated 24x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m INFO 11-13 00:50:04 [loggers.py:127] Engine 000: Avg prompt throughput: 0.0 tokens/s, Avg generation throughput: 861.7 tokens/s, Running: 27 reqs, Waiting: 0 reqs, GPU KV cache usage: 2.1%, Prefix cache hit rate: 29.4%[32m [repeated 21x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m [vLLM] Elapsed time for batch 3042bc14691149ac81ae607a724279a7 with size 12: 13.338462130001062[32m [repeated 21x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m INFO 11-13 00:50:09 [loggers.py:127] Engine 000: Avg prompt throughput: 0.0 tokens/s, Avg generation throughput: 2155.8 tokens/s, Running: 82 reqs, Waiting: 0 reqs, GPU KV cache usage: 5.3%, Prefix cache hit rate: 29.4%[32m [repeated 10x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m [vLLM] Elapsed time for batch eebc263b546740f7b38d9cd61cf6ab43 with size 12: 13.035998986999402[32m [repeated 10x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m INFO 11-13 00:50:15 [loggers.py:127] Engine 000: Avg prompt throughput: 1960.8 tokens/s, Avg generation throughput: 1295.9 tokens/s, Running: 127 reqs, Waiting: 11 reqs, GPU KV cache usage: 7.4%, Prefix cache hit rate: 29.5%[32m [repeated 10x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m [vLLM] Elapsed time for batch 6a8f5ac3fd0345c0a2d5bd1b46d3caad with size 12: 14.069810341999982[32m [repeated 10x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [vLLM] Elapsed time for batch 2aa8e5a566414df6a98986432b5d4681 with size 16: 19.24651466599971[32m [repeated 12x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m INFO 11-13 00:50:21 [loggers.py:127] Engine 000: Avg prompt throughput: 0.0 tokens/s, Avg generation throughput: 0.0 tokens/s, Running: 122 reqs, Waiting: 13 reqs, GPU KV cache usage: 7.0%, Prefix cache hit rate: 29.4%[32m [repeated 12x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [vLLM] Elapsed time for batch 4737fe13c0614f39812a1a0590108e75 with size 16: 18.361672896000528[32m [repeated 17x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m INFO 11-13 00:50:26 [loggers.py:127] Engine 000: Avg prompt throughput: 1045.1 tokens/s, Avg generation throughput: 1751.1 tokens/s, Running: 125 reqs, Waiting: 13 reqs, GPU KV cache usage: 7.5%, Prefix cache hit rate: 29.4%[32m [repeated 17x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m INFO 11-13 00:50:31 [loggers.py:127] Engine 000: Avg prompt throughput: 1500.1 tokens/s, Avg generation throughput: 1469.9 tokens/s, Running: 124 reqs, Waiting: 10 reqs, GPU KV cache usage: 7.5%, Prefix cache hit rate: 29.3%[32m [repeated 12x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m [vLLM] Elapsed time for batch 43f8a04c4aa94e0685b2c968bfbd45d8 with size 16: 22.06792594700164[32m [repeated 12x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m INFO 11-13 00:50:37 [loggers.py:127] Engine 000: Avg prompt throughput: 1396.3 tokens/s, Avg generation throughput: 1550.9 tokens/s, Running: 127 reqs, Waiting: 12 reqs, GPU KV cache usage: 7.8%, Prefix cache hit rate: 29.4%[32m [repeated 14x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m [vLLM] Elapsed time for batch a9ccb326bfde460e8b03c89cc7751faf with size 16: 22.87389754099968[32m [repeated 14x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m INFO 11-13 00:50:42 [loggers.py:127] Engine 000: Avg prompt throughput: 1441.9 tokens/s, Avg generation throughput: 1677.8 tokens/s, Running: 127 reqs, Waiting: 13 reqs, GPU KV cache usage: 7.7%, Prefix cache hit rate: 29.5%[32m [repeated 18x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m [vLLM] Elapsed time for batch 451b4098d7f04c22865390abf0ab5986 with size 16: 19.36807647600108[32m [repeated 18x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m INFO 11-13 00:50:47 [loggers.py:127] Engine 000: Avg prompt throughput: 732.7 tokens/s, Avg generation throughput: 2178.9 tokens/s, Running: 111 reqs, Waiting: 0 reqs, GPU KV cache usage: 7.1%, Prefix cache hit rate: 29.4%[32m [repeated 18x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m [vLLM] Elapsed time for batch 15b71a9a220e4a1cbb56d98be7a81695 with size 16: 23.85315748799985[32m [repeated 19x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=35865, ip=10.0.97.54)[0m INFO 11-13 00:50:52 [loggers.py:127] Engine 000: Avg prompt throughput: 0.0 tokens/s, Avg generation throughput: 1931.2 tokens/s, Running: 66 reqs, Waiting: 0 reqs, GPU KV cache usage: 3.9%, Prefix cache hit rate: 29.3%[32m [repeated 19x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m [vLLM] Elapsed time for batch 87ea90fc9e694345be4695eb2fb16bad with size 16: 20.875699045000147[32m [repeated 22x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m INFO 11-13 00:50:57 [loggers.py:127] Engine 000: Avg prompt throughput: 0.0 tokens/s, Avg generation throughput: 1367.0 tokens/s, Running: 37 reqs, Waiting: 0 reqs, GPU KV cache usage: 2.7%, Prefix cache hit rate: 29.3%[32m [repeated 29x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [1;36m(Worker pid=41360)[0;0m INFO 11-13 00:50:57 [multiproc_executor.py:558] Parent process exited, terminating worker
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [1;36m(Worker pid=41360)[0;0m INFO 11-13 00:50:57 [multiproc_executor.py:599] WorkerProc shutting down.


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m [vLLM] Elapsed time for batch 4f5e9a4331de460898f33461ba2f1197 with size 16: 16.752794754000206[32m [repeated 25x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [1;36m(Worker pid=41360)[0;0m /home/ray/anaconda3/lib/python3.11/multiprocessing/resource_tracker.py:123: UserWarning: resource_tracker: process died unexpectedly, relaunching.  Some resources might leak.
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m [1;36m(Worker pid=41360)[0;0m   warnings.warn('resource_tracker: process died unexpectedly, '
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m Traceback (most recent call last):
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m   File "/home/ray/anaconda3/lib/python3.11/multiprocessing/resource_tracker.py", line 239, in main
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m     cache[rtype].remove(name)
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m KeyError: '/psm_92cc1c28'
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m Traceback (most recent call last):
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m   File "/home/ray/anaconda3/lib/python3.11/multiprocessing/resource_tracker.py", line 239, in main
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m     cache[rtype].remove(name)
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41262, ip=10.0.70.78)[0m KeyError: '/mp-ez06z5cg'


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m INFO 11-13 00:51:03 [loggers.py:127] Engine 000: Avg prompt throughput: 0.0 tokens/s, Avg generation throughput: 437.9 tokens/s, Running: 8 reqs, Waiting: 0 reqs, GPU KV cache usage: 0.9%, Prefix cache hit rate: 29.3%[32m [repeated 19x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m [1;36m(Worker pid=37014)[0;0m INFO 11-13 00:51:02 [multiproc_executor.py:558] Parent process exited, terminating worker[32m [repeated 2x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m [1;36m(Worker pid=37014)[0;0m INFO 11-13 00:51:02 [multiproc_executor.py:599] WorkerProc shutting down.[32m [repeated 2x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m [vLLM] Elapsed time for batch 8e105171d2f740ff8933a959e797d1f5 with size 16: 10.122071268000582[32m [repeated 19x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m [1;36m(Worker pid=37014)[0;0m /home/ray/anaconda3/lib/python3.11/multiprocessing/resource_tracker.py:123: UserWarning: resource_tracker: process died unexpectedly, relaunching.  Some resources might leak.[32m [repeated 2x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m [1;36m(Worker pid=37014)[0;0m   warnings.warn('resource_tracker: process died unexpectedly, '[32m [repeated 2x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m Traceback (most recent call last):[32m [repeated 4x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m   File "/home/ray/anaconda3/lib/python3.11/multiprocessing/resource_tracker.py", line 239, in main[32m [repeated 4x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m     cache[rtype].remove(name)[32m [repeated 4x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36916, ip=10.0.96.245)[0m KeyError: '/mp-ddzkacu5'[32m [repeated 4x across cluster][0m
    2025-11-13 00:51:05,631	INFO streaming_executor.py:298 -- ✔️  Dataset dataset_213_0 execution finished in 280.92 seconds
    2025-11-13 00:51:05,705	INFO logging.py:397 -- Registered dataset logger for dataset dataset_215_0
    2025-11-13 00:51:05,706	INFO streaming_executor.py:170 -- Starting execution of Dataset dataset_215_0. Full logs are in /tmp/ray/session_2025-11-12_14-59-56_310597_2440/logs/ray-data
    2025-11-13 00:51:05,707	INFO streaming_executor.py:171 -- Execution plan of Dataset dataset_215_0: InputDataBuffer[Input] -> LimitOperator[limit=3]



    Running 0: 0.00 row [00:00, ? row/s]



    - limit=3 1: 0.00 row [00:00, ? row/s]


    2025-11-13 00:51:05,793	INFO streaming_executor.py:298 -- ✔️  Dataset dataset_215_0 execution finished in 0.06 seconds
    2025-11-13 00:51:05,802	INFO util.py:257 -- Exiting prefetcher's background thread


    
    ==================GENERATED CAPTIONS===============
    
    [{'generated_caption': 'The image features a woman with white hair and '
                           'glasses, who appears to be an elderly woman. She is '
                           'wearing a dark outfit and has a calm and composed '
                           'expression. The background is a gradient of purple and '
                           'blue hues, with a subtle floral pattern. At the top of '
                           'the image, there is text that reads "Mélanie Klein" '
                           'and "As experiências da psicologia," which translates '
                           'to "Melanie Klein" and "The experiences of '
                           'psychology." There is also a logo that includes the '
                           'word "Pensilvânia" and an image of a book, suggesting '
                           'a connection to psychology or literature. The overall '
                           'aesthetic of the image is serene and intellectual.'},
     {'generated_caption': 'The image shows a single, lit candle placed inside a '
                           'clear glass candle holder. The candle is yellow and '
                           'appears to be made of a beeswax blend, which is '
                           'evident from its golden hue and the slight '
                           'imperfections that are characteristic of beeswax. The '
                           'glass holder is rectangular and has a slightly frosted '
                           'appearance, giving it a warm, rustic look. The candle '
                           'is lit, casting a soft, warm glow that illuminates the '
                           'immediate area around it. The background is blurred, '
                           'drawing attention to the candle and its holder. The '
                           'overall setting suggests a cozy, intimate atmosphere, '
                           'possibly in a home or a cozy indoor space.'},
     {'generated_caption': 'The image depicts a weathered, textured tree trunk '
                           'with visible signs of aging and decay. The bark is '
                           'rough and has patches of dark, possibly moss or '
                           'lichen, which contrasts with the lighter, more exposed '
                           'wood. The trunk has several vertical cracks and '
                           'splits, indicating significant age and exposure to the '
                           'elements. The background is blurred, suggesting a '
                           'focus on the tree trunk itself, but it appears to be '
                           'an outdoor setting with natural light, possibly '
                           'indicating a forest or wooded area. The overall scene '
                           'conveys a sense of natural decay and the passage of '
                           'time.'}]


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m [1;36m(Worker pid=41568)[0;0m /home/ray/anaconda3/lib/python3.11/multiprocessing/resource_tracker.py:123: UserWarning: resource_tracker: process died unexpectedly, relaunching.  Some resources might leak.
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m [1;36m(Worker pid=41568)[0;0m   warnings.warn('resource_tracker: process died unexpectedly, '
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m Traceback (most recent call last):
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m   File "/home/ray/anaconda3/lib/python3.11/multiprocessing/resource_tracker.py", line 239, in main
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m     cache[rtype].remove(name)
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m KeyError: '/psm_5d5e545d'
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m Traceback (most recent call last):
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m   File "/home/ray/anaconda3/lib/python3.11/multiprocessing/resource_tracker.py", line 239, in main
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m     cache[rtype].remove(name)
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m KeyError: '/mp-_ypr3ns6'


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m [1;36m(Worker pid=41568)[0;0m INFO 11-13 00:51:05 [multiproc_executor.py:558] Parent process exited, terminating worker
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=41459, ip=10.0.107.216)[0m [1;36m(Worker pid=41568)[0;0m INFO 11-13 00:51:05 [multiproc_executor.py:599] WorkerProc shutting down.


## Launch to production with Anyscale Jobs

For production workloads, deploy your batch inference processor as an [Anyscale Job](https://docs.anyscale.com/platform/jobs). Anyscale takes care of the infrastructure layer and runs your jobs on your dedicated clusters with automatic retries, monitoring, and scheduling.

### Configure an Anyscale Job

Save your batch inference code as `batch_vision_inference.py`, then create a job configuration file:

```yaml
# job.yaml
name: llm-batch-inference-vision
entrypoint: python batch_inference_vision.py
image_uri: anyscale/ray:2.49.0-py311-cu128
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
# Check job status.
anyscale job status --name vlm-batch-inference-vision

# View logs.
anyscale job logs --name vlm-batch-inference-vision
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

Your Ray Data processing pipeline can easily scale up to process more images. The Leonardo6/datacomp10m dataset contains approximately 1 million images, making it ideal for demonstrating large-scale batch inference.

The following example processes a configurable number of images from the dataset, controlled by the `LARGE_DATASET_LIMIT` environment variable (default: 100k images). You can increase this to process the full 1M images or any subset.


```python
import os

# The Leonardo6/datacomp10m dataset has ~10M images
# Configure how many images to process (default: 2M for demonstration).
dataset_limit = int(os.environ.get("LARGE_DATASET_LIMIT", 2_000_000))
print(f"Processing {dataset_limit:,} images from the ~10M image dataset...")

# Apply the limit to the dataset.
ds_large = ds.limit(dataset_limit)

# Repartition for better parallelism.
num_partitions_large = 128
print(f"Repartitioning dataset into {num_partitions_large} blocks for parallelism...")
ds_large = ds_large.repartition(num_blocks=num_partitions_large)

# Run the same processor on the larger dataset.
processed_large = processor(ds_large)
processed_large = processed_large.materialize()

print(f"\nProcessed {processed_large.count()} images successfully.")
print("\nSample outputs:")
pprint(processed_large.take(3))
```

    2025-11-13 01:25:24,760	WARNING util.py:597 -- The argument ``concurrency`` is deprecated in Ray 2.51. Please specify argument ``compute`` instead. For more information, see https://docs.ray.io/en/master/data/transforming-data.html#stateful-transforms.
    2025-11-13 01:25:24,761	WARNING util.py:597 -- The argument ``concurrency`` is deprecated in Ray 2.51. Please specify argument ``compute`` instead. For more information, see https://docs.ray.io/en/master/data/transforming-data.html#stateful-transforms.
    2025-11-13 01:25:24,763	WARNING util.py:597 -- The argument ``concurrency`` is deprecated in Ray 2.51. Please specify argument ``compute`` instead. For more information, see https://docs.ray.io/en/master/data/transforming-data.html#stateful-transforms.
    2025-11-13 01:25:24,765	WARNING util.py:597 -- The argument ``concurrency`` is deprecated in Ray 2.51. Please specify argument ``compute`` instead. For more information, see https://docs.ray.io/en/master/data/transforming-data.html#stateful-transforms.
    2025-11-13 01:25:24,770	INFO logging.py:397 -- Registered dataset logger for dataset dataset_247_0
    2025-11-13 01:25:24,775	INFO streaming_executor.py:170 -- Starting execution of Dataset dataset_247_0. Full logs are in /tmp/ray/session_2025-11-12_14-59-56_310597_2440/logs/ray-data
    2025-11-13 01:25:24,776	INFO streaming_executor.py:171 -- Execution plan of Dataset dataset_247_0: InputDataBuffer[Input] -> TaskPoolMapOperator[ReadHuggingFace] -> LimitOperator[limit=2000000] -> TaskPoolMapOperator[Map(normalize_row)] -> AllToAllOperator[Repartition] -> ActorPoolMapOperator[Map(_preprocess)->MapBatches(PrepareImageUDF)] -> ActorPoolMapOperator[MapBatches(ChatTemplateUDF)] -> ActorPoolMapOperator[MapBatches(TokenizeUDF)] -> ActorPoolMapOperator[MapBatches(vLLMEngineStageUDF)] -> ActorPoolMapOperator[MapBatches(DetokenizeUDF)] -> TaskPoolMapOperator[Map(_postprocess)]


    Processing 2,000,000 images from the ~10M image dataset...
    Repartitioning dataset into 128 blocks for parallelism...



    Running 0: 0.00 row [00:00, ? row/s]



    - ReadHuggingFace->SplitBlocks(200) 1: 0.00 row [00:00, ? row/s]



    - limit=2000000 2: 0.00 row [00:00, ? row/s]



    - Map(normalize_row) 3: 0.00 row [00:00, ? row/s]



    - Repartition 4: 0.00 row [00:00, ? row/s]



    Split Repartition 5:   0%|          | 0.00/1.00 [00:00<?, ? row/s]



    - Map(_preprocess)->MapBatches(PrepareImageUDF) 6: 0.00 row [00:00, ? row/s]



    - MapBatches(ChatTemplateUDF) 7: 0.00 row [00:00, ? row/s]



    - MapBatches(TokenizeUDF) 8: 0.00 row [00:00, ? row/s]



    - MapBatches(vLLMEngineStageUDF) 9: 0.00 row [00:00, ? row/s]



    - MapBatches(DetokenizeUDF) 10: 0.00 row [00:00, ? row/s]



    - Map(_postprocess) 11: 0.00 row [00:00, ? row/s]


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=46742, ip=10.0.67.56)[0m You have video processor config saved in `preprocessor.json` file which is deprecated. Video processor configs should be saved in their own `video_preprocessor.json` file. You can rename the file or load and save the processor back which renames it automatically. Loading from `preprocessor.json` will be removed in v5.0.[32m [repeated 2x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=48193, ip=10.0.110.182)[0m [1;36m(Worker pid=48296)[0;0m 


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=46742, ip=10.0.67.56)[0m [1;36m(Worker pid=46839)[0;0m INFO 11-13 01:25:22 [gpu_model_runner.py:2602] Starting to load model Qwen/Qwen2.5-VL-3B-Instruct...[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=46742, ip=10.0.67.56)[0m [1;36m(Worker pid=46839)[0;0m INFO 11-13 01:25:22 [gpu_model_runner.py:2634] Loading model from scratch...[32m [repeated 3x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m Max pending requests is set to 141
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m Downloading model and tokenizer.
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m No cloud storage mirror configured
    Loading safetensors checkpoint shards:   0% Completed | 0/2 [00:00<?, ?it/s][32m [repeated 3x across cluster][0m
    Loading safetensors checkpoint shards: 100% Completed | 2/2 [00:03<00:00,  1.77s/it][32m [repeated 11x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m INFO 11-13 01:25:29 [__init__.py:216] Automatically detected platform cuda.
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=46742, ip=10.0.67.56)[0m [1;36m(Worker pid=46839)[0;0m INFO 11-13 01:25:22 [cuda.py:366] Using Flash Attention backend on V1 engine.[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=46742, ip=10.0.67.56)[0m [1;36m(Worker pid=46839)[0;0m INFO 11-13 01:25:23 [weight_utils.py:392] Using model weights format ['*.safetensors'][32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=48193, ip=10.0.110.182)[0m [1;36m(Worker pid=48296)[0;0m INFO 11-13 01:25:26 [default_loader.py:267] Loading weights took 3.67 seconds[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=48193, ip=10.0.110.182)[0m [1;36m(Worker pid=48296)[0;0m INFO 11-13 01:25:26 [gpu_model_runner.py:2653] Model loading took 7.1550 GiB and 4.216287 seconds[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=48193, ip=10.0.110.182)[0m [1;36m(Worker pid=48296)[0;0m INFO 11-13 01:25:26 [gpu_model_runner.py:3344] Encoder cache will be initialized with a budget of 16384 tokens, and profiled with 1 image items of the maximum feature size.[32m [repeated 3x across cluster][0m


    [36m(MapWorker(MapBatches(ChatTemplateUDF)) pid=43998, ip=10.0.125.65)[0m The image processor of type `Qwen2VLImageProcessor` is now loaded as a fast processor by default, even if the model checkpoint was saved with a slow processor. This is a breaking change and may produce slightly different outputs. To continue using the slow processor, instantiate this class with `use_fast=False`. Note that this behavior will be extended to all models in a future release.
    [36m(MapWorker(MapBatches(ChatTemplateUDF)) pid=43998, ip=10.0.125.65)[0m You have video processor config saved in `preprocessor.json` file which is deprecated. Video processor configs should be saved in their own `video_preprocessor.json` file. You can rename the file or load and save the processor back which renames it automatically. Loading from `preprocessor.json` will be removed in v5.0.


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m INFO 11-13 01:25:31 [model.py:547] Resolved architecture: Qwen2_5_VLForConditionalGeneration
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m INFO 11-13 01:25:31 [model.py:1510] Using max model len 8192
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m INFO 11-13 01:25:31 [arg_utils.py:1215] Using ray runtime env: {'_ray_commit': 'aceca04cbbf42e2c50a7e523108a35961c1a612e', 'cgroupv2': {}, 'ray_debugger': {'working_dir': '/home/ray/default'}, 'working_dir': 'gcs://_ray_pkg_71ce8c148e2944680e6a5fd1071541790989e709.zip', 'pip': {'packages': ['datasets'], 'pip_check': False}, 'env_vars': {'LARGE_DATASET_LIMIT': '10000'}}
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m INFO 11-13 01:25:31 [scheduler.py:205] Chunked prefill is enabled with max_num_batched_tokens=2048.
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m WARNING 11-13 01:25:34 [__init__.py:3036] We must use the `spawn` multiprocessing start method. Overriding VLLM_WORKER_MULTIPROC_METHOD to 'spawn'. See https://docs.vllm.ai/en/latest/usage/troubleshooting.html#python-multiprocessing for more information. Reasons: In a Ray actor and can only be spawned
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49180, ip=10.0.107.216)[0m INFO 11-13 01:25:29 [__init__.py:216] Automatically detected platform cuda.
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49063, ip=10.0.70.78)[0m [1;36m(Worker pid=49154)[0;0m INFO 11-13 01:25:37 [backends.py:548] Using cache directory: /home/ray/.cache/vllm/torch_compile_cache/a314dc5b3a/rank_0_0/backbone for vLLM's torch.compile
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49063, ip=10.0.70.78)[0m [1;36m(Worker pid=49154)[0;0m INFO 11-13 01:25:37 [backends.py:559] Dynamo bytecode transform time: 6.26 s
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49180, ip=10.0.107.216)[0m INFO 11-13 01:25:32 [model.py:547] Resolved architecture: Qwen2_5_VLForConditionalGeneration[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49180, ip=10.0.107.216)[0m INFO 11-13 01:25:32 [model.py:1510] Using max model len 8192[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49180, ip=10.0.107.216)[0m INFO 11-13 01:25:32 [arg_utils.py:1215] Using ray runtime env: {'_ray_commit': 'aceca04cbbf42e2c50a7e523108a35961c1a612e', 'cgroupv2': {}, 'ray_debugger': {'working_dir': '/home/ray/default'}, 'working_dir': 'gcs://_ray_pkg_71ce8c148e2944680e6a5fd1071541790989e709.zip', 'pip': {'packages': ['datasets'], 'pip_check': False}, 'env_vars': {'LARGE_DATASET_LIMIT': '10000'}}[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49180, ip=10.0.107.216)[0m INFO 11-13 01:25:32 [scheduler.py:205] Chunked prefill is enabled with max_num_batched_tokens=2048.[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m INFO 11-13 01:25:37 [__init__.py:216] Automatically detected platform cuda.
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m [1;36m(EngineCore_DP0 pid=43065)[0;0m INFO 11-13 01:25:39 [core.py:644] Waiting for init message from front-end.
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m [1;36m(EngineCore_DP0 pid=43065)[0;0m INFO 11-13 01:25:39 [core.py:77] Initializing a V1 LLM engine (v0.11.0) with config: model='Qwen/Qwen2.5-VL-3B-Instruct', speculative_config=None, tokenizer='Qwen/Qwen2.5-VL-3B-Instruct', skip_tokenizer_init=False, tokenizer_mode=auto, revision=None, tokenizer_revision=None, trust_remote_code=False, dtype=torch.bfloat16, max_seq_len=8192, download_dir=None, load_format=auto, tensor_parallel_size=1, pipeline_parallel_size=1, data_parallel_size=1, disable_custom_all_reduce=False, quantization=None, enforce_eager=False, kv_cache_dtype=auto, device_config=cuda, structured_outputs_config=StructuredOutputsConfig(backend='auto', disable_fallback=False, disable_any_whitespace=False, disable_additional_properties=False, reasoning_parser=''), observability_config=ObservabilityConfig(show_hidden_metrics_for_version=None, otlp_traces_endpoint=None, collect_detailed_traces=None), seed=0, served_model_name=Qwen/Qwen2.5-VL-3B-Instruct, enable_prefix_caching=True, chunked_prefill_enabled=True, pooler_config=None, compilation_config={"level":3,"debug_dump_path":"","cache_dir":"","backend":"","custom_ops":[],"splitting_ops":["vllm.unified_attention","vllm.unified_attention_with_output","vllm.mamba_mixer2","vllm.mamba_mixer","vllm.short_conv","vllm.linear_attention","vllm.plamo2_mamba_mixer","vllm.gdn_attention","vllm.sparse_attn_indexer"],"use_inductor":true,"compile_sizes":[],"inductor_compile_config":{"enable_auto_functionalized_v2":false},"inductor_passes":{},"cudagraph_mode":[2,1],"use_cudagraph":true,"cudagraph_num_of_warmups":1,"cudagraph_capture_sizes":[256,248,240,232,224,216,208,200,192,184,176,168,160,152,144,136,128,120,112,104,96,88,80,72,64,56,48,40,32,24,16,8,4,2,1],"cudagraph_copy_inputs":false,"full_cuda_graph":false,"use_inductor_graph_partition":false,"pass_config":{},"max_capture_size":256,"local_cache_dir":null}
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m [1;36m(EngineCore_DP0 pid=43065)[0;0m INFO 11-13 01:25:39 [shm_broadcast.py:289] vLLM message queue communication handle: Handle(local_reader_ranks=[0], buffer_handle=(1, 16777216, 10, 'psm_2025443d'), local_subscribe_addr='ipc:///tmp/b66f4682-0482-4e36-a755-e95013ef86aa', remote_subscribe_addr=None, remote_addr_ipv6=False)
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49063, ip=10.0.70.78)[0m [1;36m(Worker pid=49154)[0;0m INFO 11-13 01:25:39 [backends.py:164] Directly load the compiled graph(s) for dynamic shape from the cache, took 1.665 s
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49063, ip=10.0.70.78)[0m [1;36m(Worker pid=49154)[0;0m INFO 11-13 01:25:40 [monitor.py:34] torch.compile takes 6.26 s in total
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49180, ip=10.0.107.216)[0m WARNING 11-13 01:25:34 [__init__.py:3036] We must use the `spawn` multiprocessing start method. Overriding VLLM_WORKER_MULTIPROC_METHOD to 'spawn'. See https://docs.vllm.ai/en/latest/usage/troubleshooting.html#python-multiprocessing for more information. Reasons: In a Ray actor and can only be spawned
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49063, ip=10.0.70.78)[0m [1;36m(Worker pid=49154)[0;0m INFO 11-13 01:25:40 [gpu_worker.py:298] Available KV cache memory: 9.26 GiB
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49063, ip=10.0.70.78)[0m [1;36m(EngineCore_DP0 pid=49136)[0;0m INFO 11-13 01:25:41 [kv_cache_utils.py:1087] GPU KV cache size: 269,568 tokens
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49063, ip=10.0.70.78)[0m [1;36m(EngineCore_DP0 pid=49136)[0;0m INFO 11-13 01:25:41 [kv_cache_utils.py:1091] Maximum concurrency for 8,192 tokens per request: 32.91x


    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):   0%|          | 0/35 [00:00<?, ?it/s]d=49154)[0;0m 
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49180, ip=10.0.107.216)[0m Max pending requests is set to 141
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49180, ip=10.0.107.216)[0m Downloading model and tokenizer.
    [36m(MapWorker(MapBatches(DetokenizeUDF)) pid=48432, ip=10.0.97.152)[0m No cloud storage mirror configured[32m [repeated 4x across cluster][0m
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):   6%|▌         | 2/35 [00:00<00:02, 12.29it/s]
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  11%|█▏        | 4/35 [00:00<00:02, 13.09it/s]
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  17%|█▋        | 6/35 [00:00<00:02, 13.51it/s]
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  23%|██▎       | 8/35 [00:00<00:01, 13.77it/s]
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  29%|██▊       | 10/35 [00:00<00:01, 13.95it/s]
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  34%|███▍      | 12/35 [00:00<00:01, 14.02it/s]
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  40%|████      | 14/35 [00:01<00:01, 13.67it/s]
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  46%|████▌     | 16/35 [00:01<00:01, 13.84it/s]


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=48193, ip=10.0.110.182)[0m [1;36m(Worker pid=48296)[0;0m INFO 11-13 01:25:40 [backends.py:548] Using cache directory: /home/ray/.cache/vllm/torch_compile_cache/a314dc5b3a/rank_0_0/backbone for vLLM's torch.compile[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=48193, ip=10.0.110.182)[0m [1;36m(Worker pid=48296)[0;0m INFO 11-13 01:25:40 [backends.py:559] Dynamo bytecode transform time: 6.49 s[32m [repeated 3x across cluster][0m


    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  91%|█████████▏| 32/35 [00:02<00:00, 14.68it/s]
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  97%|█████████▋| 34/35 [00:02<00:00, 14.86it/s]


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49180, ip=10.0.107.216)[0m INFO 11-13 01:25:42 [__init__.py:216] Automatically detected platform cuda.[32m [repeated 3x across cluster][0m


    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE): 100%|██████████| 35/35 [00:02<00:00, 14.07it/s]
    Capturing CUDA graphs (decode, FULL):   0%|          | 0/19 [00:00<?, ?it/s]m [1;36m(Worker pid=49154)[0;0m 
    Capturing CUDA graphs (decode, FULL):  11%|█         | 2/19 [00:00<00:01, 15.31it/s]
    Capturing CUDA graphs (decode, FULL):  21%|██        | 4/19 [00:00<00:00, 15.81it/s]


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49180, ip=10.0.107.216)[0m [1;36m(EngineCore_DP0 pid=49264)[0;0m INFO 11-13 01:25:39 [core.py:644] Waiting for init message from front-end.
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49180, ip=10.0.107.216)[0m [1;36m(EngineCore_DP0 pid=49264)[0;0m INFO 11-13 01:25:39 [core.py:77] Initializing a V1 LLM engine (v0.11.0) with config: model='Qwen/Qwen2.5-VL-3B-Instruct', speculative_config=None, tokenizer='Qwen/Qwen2.5-VL-3B-Instruct', skip_tokenizer_init=False, tokenizer_mode=auto, revision=None, tokenizer_revision=None, trust_remote_code=False, dtype=torch.bfloat16, max_seq_len=8192, download_dir=None, load_format=auto, tensor_parallel_size=1, pipeline_parallel_size=1, data_parallel_size=1, disable_custom_all_reduce=False, quantization=None, enforce_eager=False, kv_cache_dtype=auto, device_config=cuda, structured_outputs_config=StructuredOutputsConfig(backend='auto', disable_fallback=False, disable_any_whitespace=False, disable_additional_properties=False, reasoning_parser=''), observability_config=ObservabilityConfig(show_hidden_metrics_for_version=None, otlp_traces_endpoint=None, collect_detailed_traces=None), seed=0, served_model_name=Qwen/Qwen2.5-VL-3B-Instruct, enable_prefix_caching=True, chunked_prefill_enabled=True, pooler_config=None, compilation_config={"level":3,"debug_dump_path":"","cache_dir":"","backend":"","custom_ops":[],"splitting_ops":["vllm.unified_attention","vllm.unified_attention_with_output","vllm.mamba_mixer2","vllm.mamba_mixer","vllm.short_conv","vllm.linear_attention","vllm.plamo2_mamba_mixer","vllm.gdn_attention","vllm.sparse_attn_indexer"],"use_inductor":true,"compile_sizes":[],"inductor_compile_config":{"enable_auto_functionalized_v2":false},"inductor_passes":{},"cudagraph_mode":[2,1],"use_cudagraph":true,"cudagraph_num_of_warmups":1,"cudagraph_capture_sizes":[256,248,240,232,224,216,208,200,192,184,176,168,160,152,144,136,128,120,112,104,96,88,80,72,64,56,48,40,32,24,16,8,4,2,1],"cudagraph_copy_inputs":false,"full_cuda_graph":false,"use_inductor_graph_partition":false,"pass_config":{},"max_capture_size":256,"local_cache_dir":null}
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49180, ip=10.0.107.216)[0m [1;36m(EngineCore_DP0 pid=49264)[0;0m INFO 11-13 01:25:39 [shm_broadcast.py:289] vLLM message queue communication handle: Handle(local_reader_ranks=[0], buffer_handle=(1, 16777216, 10, 'psm_0228d41b'), local_subscribe_addr='ipc:///tmp/ef730e84-5138-4d7d-9623-3a76c75bcec5', remote_subscribe_addr=None, remote_addr_ipv6=False)


    Capturing CUDA graphs (decode, FULL):  32%|███▏      | 6/19 [00:00<00:00, 16.08it/s]
    Capturing CUDA graphs (decode, FULL):  42%|████▏     | 8/19 [00:00<00:00, 16.26it/s]
    Capturing CUDA graphs (decode, FULL):  53%|█████▎    | 10/19 [00:00<00:00, 16.35it/s]
    Capturing CUDA graphs (decode, FULL):  63%|██████▎   | 12/19 [00:00<00:00, 16.33it/s]
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE): 100%|██████████| 35/35 [00:02<00:00, 15.30it/s]
    Capturing CUDA graphs (decode, FULL):  74%|███████▎  | 14/19 [00:00<00:00, 16.47it/s]
    Capturing CUDA graphs (decode, FULL):  84%|████████▍ | 16/19 [00:00<00:00, 16.62it/s]
    Capturing CUDA graphs (decode, FULL): 100%|██████████| 19/19 [00:01<00:00, 16.51it/s]


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m INFO 11-13 01:25:45 [shm_broadcast.py:289] vLLM message queue communication handle: Handle(local_reader_ranks=[0], buffer_handle=(1, 10485760, 10, 'psm_4c0eea81'), local_subscribe_addr='ipc:///tmp/042c844c-7b7b-4076-8ce1-30484c904461', remote_subscribe_addr=None, remote_addr_ipv6=False)
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=48193, ip=10.0.110.182)[0m [1;36m(Worker pid=48296)[0;0m INFO 11-13 01:25:42 [backends.py:164] Directly load the compiled graph(s) for dynamic shape from the cache, took 1.677 s[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=48193, ip=10.0.110.182)[0m [1;36m(Worker pid=48296)[0;0m INFO 11-13 01:25:42 [monitor.py:34] torch.compile takes 6.49 s in total[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49063, ip=10.0.70.78)[0m [1;36m(Worker pid=49154)[0;0m INFO 11-13 01:25:45 [gpu_model_runner.py:3480] Graph capturing finished in 4 secs, took 0.44 GiB
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49063, ip=10.0.70.78)[0m [1;36m(EngineCore_DP0 pid=49136)[0;0m INFO 11-13 01:25:45 [core.py:210] init engine (profile, create kv cache, warmup model) took 21.45 seconds


    Capturing CUDA graphs (decode, FULL):  95%|█████████▍| 18/19 [00:01<00:00, 18.47it/s]
    Capturing CUDA graphs (decode, FULL): 100%|██████████| 19/19 [00:01<00:00, 18.02it/s]


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m [Gloo] Rank 0 is connected to 0 peer ranks. Expected number of connected peer ranks is : 0
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m [Gloo] Rank 0 is connected to 0 peer ranks. Expected number of connected peer ranks is : 0
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m [Gloo] Rank 0 is connected to 0 peer ranks. Expected number of connected peer ranks is : 0
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m [Gloo] Rank 0 is connected to 0 peer ranks. Expected number of connected peer ranks is : 0
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m [Gloo] Rank 0 is connected to 0 peer ranks. Expected number of connected peer ranks is : 0
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m [Gloo] Rank 0 is connected to 0 peer ranks. Expected number of connected peer ranks is : 0
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m INFO 11-13 01:25:46 [parallel_state.py:1208] rank 0 in world size 1 is assigned as DP rank 0, PP rank 0, TP rank 0, EP rank 0
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=48193, ip=10.0.110.182)[0m [1;36m(Worker pid=48296)[0;0m INFO 11-13 01:25:43 [gpu_worker.py:298] Available KV cache memory: 9.26 GiB[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=48193, ip=10.0.110.182)[0m [1;36m(EngineCore_DP0 pid=48273)[0;0m INFO 11-13 01:25:44 [kv_cache_utils.py:1087] GPU KV cache size: 269,568 tokens[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=48193, ip=10.0.110.182)[0m [1;36m(EngineCore_DP0 pid=48273)[0;0m INFO 11-13 01:25:44 [kv_cache_utils.py:1091] Maximum concurrency for 8,192 tokens per request: 32.91x[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m WARNING 11-13 01:25:46 [topk_topp_sampler.py:66] FlashInfer is not available. Falling back to the PyTorch-native implementation of top-p & top-k sampling. For the best performance, please install FlashInfer.


    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):   0%|          | 0/35 [00:00<?, ?it/s][32m [repeated 3x across cluster][0m
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  86%|████████▌ | 30/35 [00:01<00:00, 15.81it/s][32m [repeated 52x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m You have video processor config saved in `preprocessor.json` file which is deprecated. Video processor configs should be saved in their own `video_preprocessor.json` file. You can rename the file or load and save the processor back which renames it automatically. Loading from `preprocessor.json` will be removed in v5.0.


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m [1;36m(Worker pid=43096)[0;0m INFO 11-13 01:25:48 [gpu_model_runner.py:2602] Starting to load model Qwen/Qwen2.5-VL-3B-Instruct...


    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE): 100%|██████████| 35/35 [00:02<00:00, 15.17it/s][32m [repeated 7x across cluster][0m
    Capturing CUDA graphs (decode, FULL):   0%|          | 0/19 [00:00<?, ?it/s][32m [repeated 3x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m [1;36m(Worker pid=43096)[0;0m INFO 11-13 01:25:49 [gpu_model_runner.py:2634] Loading model from scratch...
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m [1;36m(Worker pid=43096)[0;0m INFO 11-13 01:25:49 [cuda.py:366] Using Flash Attention backend on V1 engine.
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m [1;36m(Worker pid=43096)[0;0m INFO 11-13 01:25:49 [weight_utils.py:392] Using model weights format ['*.safetensors']


    Loading safetensors checkpoint shards:   0% Completed | 0/2 [00:00<?, ?it/s]m [1;36m(Worker pid=43096)[0;0m 
    Capturing CUDA graphs (decode, FULL):  84%|████████▍ | 16/19 [00:00<00:00, 18.02it/s][32m [repeated 24x across cluster][0m
    Loading safetensors checkpoint shards:  50% Completed | 1/2 [00:00<00:00,  2.29it/s]m(Worker pid=43096)[0;0m 


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49063, ip=10.0.70.78)[0m INFO 11-13 01:25:50 [loggers.py:147] Engine 000: vllm cache_config_info with initialization after num_gpu_blocks is: 16848
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49180, ip=10.0.107.216)[0m INFO 11-13 01:25:45 [shm_broadcast.py:289] vLLM message queue communication handle: Handle(local_reader_ranks=[0], buffer_handle=(1, 10485760, 10, 'psm_b46708c0'), local_subscribe_addr='ipc:///tmp/96a3f7c7-6e9e-434d-a53a-ddacb18d38bd', remote_subscribe_addr=None, remote_addr_ipv6=False)
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=48193, ip=10.0.110.182)[0m [1;36m(Worker pid=48296)[0;0m INFO 11-13 01:25:48 [gpu_model_runner.py:3480] Graph capturing finished in 4 secs, took 0.44 GiB[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=48193, ip=10.0.110.182)[0m [1;36m(EngineCore_DP0 pid=48273)[0;0m INFO 11-13 01:25:48 [core.py:210] init engine (profile, create kv cache, warmup model) took 21.47 seconds[32m [repeated 3x across cluster][0m


    Loading safetensors checkpoint shards: 100% Completed | 2/2 [00:01<00:00,  1.80it/s]m(Worker pid=43096)[0;0m 
    Loading safetensors checkpoint shards: 100% Completed | 2/2 [00:01<00:00,  1.86it/s]m(Worker pid=43096)[0;0m 
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m [1;36m(Worker pid=43096)[0;0m 
    Capturing CUDA graphs (decode, FULL): 100%|██████████| 19/19 [00:01<00:00, 17.61it/s][32m [repeated 4x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m [1;36m(Worker pid=43096)[0;0m INFO 11-13 01:25:50 [default_loader.py:267] Loading weights took 1.22 seconds
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m [1;36m(Worker pid=43096)[0;0m INFO 11-13 01:25:51 [gpu_model_runner.py:2653] Model loading took 7.1550 GiB and 1.910123 seconds
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49180, ip=10.0.107.216)[0m [Gloo] Rank 0 is connected to 0 peer ranks. Expected number of connected peer ranks is : 0[32m [repeated 6x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49180, ip=10.0.107.216)[0m INFO 11-13 01:25:46 [parallel_state.py:1208] rank 0 in world size 1 is assigned as DP rank 0, PP rank 0, TP rank 0, EP rank 0
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49180, ip=10.0.107.216)[0m WARNING 11-13 01:25:46 [topk_topp_sampler.py:66] FlashInfer is not available. Falling back to the PyTorch-native implementation of top-p & top-k sampling. For the best performance, please install FlashInfer.


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49180, ip=10.0.107.216)[0m [1;36m(Worker pid=49305)[0;0m 


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m [1;36m(Worker pid=43096)[0;0m INFO 11-13 01:25:51 [gpu_model_runner.py:3344] Encoder cache will be initialized with a budget of 16384 tokens, and profiled with 1 image items of the maximum feature size.
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m [1;36m(Worker pid=43096)[0;0m INFO 11-13 01:26:05 [backends.py:548] Using cache directory: /home/ray/.cache/vllm/torch_compile_cache/a314dc5b3a/rank_0_0/backbone for vLLM's torch.compile
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m [1;36m(Worker pid=43096)[0;0m INFO 11-13 01:26:05 [backends.py:559] Dynamo bytecode transform time: 6.42 s
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49180, ip=10.0.107.216)[0m [1;36m(Worker pid=49305)[0;0m INFO 11-13 01:25:49 [gpu_model_runner.py:2602] Starting to load model Qwen/Qwen2.5-VL-3B-Instruct...
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49180, ip=10.0.107.216)[0m [1;36m(Worker pid=49305)[0;0m INFO 11-13 01:25:49 [gpu_model_runner.py:2634] Loading model from scratch...
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49180, ip=10.0.107.216)[0m [1;36m(Worker pid=49305)[0;0m INFO 11-13 01:25:49 [cuda.py:366] Using Flash Attention backend on V1 engine.
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49180, ip=10.0.107.216)[0m [1;36m(Worker pid=49305)[0;0m INFO 11-13 01:25:50 [weight_utils.py:392] Using model weights format ['*.safetensors']
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=48193, ip=10.0.110.182)[0m INFO 11-13 01:25:53 [loggers.py:147] Engine 000: vllm cache_config_info with initialization after num_gpu_blocks is: 16848[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49180, ip=10.0.107.216)[0m [1;36m(Worker pid=49305)[0;0m INFO 11-13 01:25:51 [default_loader.py:267] Loading weights took 1.22 seconds
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49180, ip=10.0.107.216)[0m [1;36m(Worker pid=49305)[0;0m INFO 11-13 01:25:52 [gpu_model_runner.py:2653] Model loading took 7.1550 GiB and 1.922834 seconds
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49180, ip=10.0.107.216)[0m [1;36m(Worker pid=49305)[0;0m INFO 11-13 01:25:52 [gpu_model_runner.py:3344] Encoder cache will be initialized with a budget of 16384 tokens, and profiled with 1 image items of the maximum feature size.
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m [1;36m(Worker pid=43096)[0;0m INFO 11-13 01:26:06 [backends.py:164] Directly load the compiled graph(s) for dynamic shape from the cache, took 1.647 s
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m [1;36m(Worker pid=43096)[0;0m INFO 11-13 01:26:07 [monitor.py:34] torch.compile takes 6.42 s in total
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m [1;36m(Worker pid=43096)[0;0m INFO 11-13 01:26:08 [gpu_worker.py:298] Available KV cache memory: 9.26 GiB
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m [1;36m(EngineCore_DP0 pid=43065)[0;0m INFO 11-13 01:26:08 [kv_cache_utils.py:1087] GPU KV cache size: 269,568 tokens
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m [1;36m(EngineCore_DP0 pid=43065)[0;0m INFO 11-13 01:26:08 [kv_cache_utils.py:1091] Maximum concurrency for 8,192 tokens per request: 32.91x


    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):   0%|          | 0/35 [00:00<?, ?it/s]d=43096)[0;0m 
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=48193, ip=10.0.110.182)[0m [1;36m(EngineCore_DP0 pid=48273)[0;0m You have video processor config saved in `preprocessor.json` file which is deprecated. Video processor configs should be saved in their own `video_preprocessor.json` file. You can rename the file or load and save the processor back which renames it automatically. Loading from `preprocessor.json` will be removed in v5.0.[32m [repeated 5x across cluster][0m
    Loading safetensors checkpoint shards:   0% Completed | 0/2 [00:00<?, ?it/s][0m [1;36m(Worker pid=49305)[0;0m 
    Loading safetensors checkpoint shards: 100% Completed | 2/2 [00:01<00:00,  1.84it/s][32m [repeated 3x across cluster][0m
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):   6%|▌         | 2/35 [00:00<00:02, 13.54it/s]
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  11%|█▏        | 4/35 [00:00<00:02, 14.21it/s]
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  17%|█▋        | 6/35 [00:00<00:01, 14.60it/s]
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  23%|██▎       | 8/35 [00:00<00:01, 14.69it/s]
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  29%|██▊       | 10/35 [00:00<00:01, 14.79it/s]
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  34%|███▍      | 12/35 [00:00<00:01, 14.90it/s]
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  40%|████      | 14/35 [00:00<00:01, 14.53it/s]
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  91%|█████████▏| 32/35 [00:02<00:00, 15.92it/s]
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  97%|█████████▋| 34/35 [00:02<00:00, 16.15it/s]
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE): 100%|██████████| 35/35 [00:02<00:00, 15.12it/s]
    Capturing CUDA graphs (decode, FULL):   0%|          | 0/19 [00:00<?, ?it/s]m [1;36m(Worker pid=43096)[0;0m 
    Capturing CUDA graphs (decode, FULL):  11%|█         | 2/19 [00:00<00:01, 15.99it/s]
    Capturing CUDA graphs (decode, FULL):  21%|██        | 4/19 [00:00<00:00, 16.37it/s]
    Capturing CUDA graphs (decode, FULL):  32%|███▏      | 6/19 [00:00<00:00, 16.76it/s]
    Capturing CUDA graphs (decode, FULL):  42%|████▏     | 8/19 [00:00<00:00, 17.04it/s]
    Capturing CUDA graphs (decode, FULL):  53%|█████▎    | 10/19 [00:00<00:00, 17.47it/s]
    Capturing CUDA graphs (decode, FULL):  63%|██████▎   | 12/19 [00:00<00:00, 17.63it/s]
    Capturing CUDA graphs (decode, FULL):  74%|███████▎  | 14/19 [00:00<00:00, 17.76it/s]
    Capturing CUDA graphs (decode, FULL):  84%|████████▍ | 16/19 [00:00<00:00, 18.13it/s]
    Capturing CUDA graphs (decode, FULL):  95%|█████████▍| 18/19 [00:01<00:00, 18.35it/s]
    Capturing CUDA graphs (decode, FULL): 100%|██████████| 19/19 [00:01<00:00, 17.79it/s]


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m [1;36m(Worker pid=43096)[0;0m INFO 11-13 01:26:12 [gpu_model_runner.py:3480] Graph capturing finished in 4 secs, took 0.44 GiB
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m [1;36m(EngineCore_DP0 pid=43065)[0;0m INFO 11-13 01:26:12 [core.py:210] init engine (profile, create kv cache, warmup model) took 21.40 seconds
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49180, ip=10.0.107.216)[0m [1;36m(Worker pid=49305)[0;0m INFO 11-13 01:26:05 [backends.py:548] Using cache directory: /home/ray/.cache/vllm/torch_compile_cache/a314dc5b3a/rank_0_0/backbone for vLLM's torch.compile
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49180, ip=10.0.107.216)[0m [1;36m(Worker pid=49305)[0;0m INFO 11-13 01:26:05 [backends.py:559] Dynamo bytecode transform time: 6.44 s
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49180, ip=10.0.107.216)[0m [1;36m(Worker pid=49305)[0;0m INFO 11-13 01:26:07 [backends.py:164] Directly load the compiled graph(s) for dynamic shape from the cache, took 1.694 s
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49180, ip=10.0.107.216)[0m [1;36m(Worker pid=49305)[0;0m INFO 11-13 01:26:08 [monitor.py:34] torch.compile takes 6.44 s in total


    Capturing CUDA graphs (decode, FULL): 100%|██████████| 19/19 [00:01<00:00, 17.56it/s]


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49180, ip=10.0.107.216)[0m [1;36m(Worker pid=49305)[0;0m INFO 11-13 01:26:09 [gpu_worker.py:298] Available KV cache memory: 9.26 GiB


    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):   0%|          | 0/35 [00:00<?, ?it/s]pid=49305)[0;0m 
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m [1;36m(EngineCore_DP0 pid=43065)[0;0m You have video processor config saved in `preprocessor.json` file which is deprecated. Video processor configs should be saved in their own `video_preprocessor.json` file. You can rename the file or load and save the processor back which renames it automatically. Loading from `preprocessor.json` will be removed in v5.0.
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  86%|████████▌ | 30/35 [00:01<00:00, 16.09it/s][32m [repeated 23x across cluster][0m
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE): 100%|██████████| 35/35 [00:02<00:00, 15.42it/s][32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49180, ip=10.0.107.216)[0m [1;36m(EngineCore_DP0 pid=49264)[0;0m You have video processor config saved in `preprocessor.json` file which is deprecated. Video processor configs should be saved in their own `video_preprocessor.json` file. You can rename the file or load and save the processor back which renames it automatically. Loading from `preprocessor.json` will be removed in v5.0.
    Capturing CUDA graphs (decode, FULL):   0%|          | 0/19 [00:00<?, ?it/s][0m [1;36m(Worker pid=49305)[0;0m 
    Capturing CUDA graphs (decode, FULL):  84%|████████▍ | 16/19 [00:00<00:00, 18.04it/s][32m [repeated 8x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m INFO 11-13 01:26:18 [loggers.py:147] Engine 000: vllm cache_config_info with initialization after num_gpu_blocks is: 16848
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49180, ip=10.0.107.216)[0m [1;36m(EngineCore_DP0 pid=49264)[0;0m INFO 11-13 01:26:09 [kv_cache_utils.py:1087] GPU KV cache size: 269,568 tokens
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49180, ip=10.0.107.216)[0m [1;36m(EngineCore_DP0 pid=49264)[0;0m INFO 11-13 01:26:09 [kv_cache_utils.py:1091] Maximum concurrency for 8,192 tokens per request: 32.91x
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49180, ip=10.0.107.216)[0m [1;36m(Worker pid=49305)[0;0m INFO 11-13 01:26:13 [gpu_model_runner.py:3480] Graph capturing finished in 4 secs, took 0.44 GiB
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49180, ip=10.0.107.216)[0m [1;36m(EngineCore_DP0 pid=49264)[0;0m INFO 11-13 01:26:13 [core.py:210] init engine (profile, create kv cache, warmup model) took 21.56 seconds
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=38737, ip=10.0.97.152)[0m [1;36m(Worker pid=38837)[0;0m INFO 11-13 01:26:32 [multiproc_executor.py:558] Parent process exited, terminating worker
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=38737, ip=10.0.97.152)[0m [1;36m(Worker pid=38837)[0;0m INFO 11-13 01:26:32 [multiproc_executor.py:599] WorkerProc shutting down.
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=49180, ip=10.0.107.216)[0m INFO 11-13 01:26:19 [loggers.py:147] Engine 000: vllm cache_config_info with initialization after num_gpu_blocks is: 16848


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=38737, ip=10.0.97.152)[0m [1;36m(Worker pid=38837)[0;0m /home/ray/anaconda3/lib/python3.11/multiprocessing/resource_tracker.py:123: UserWarning: resource_tracker: process died unexpectedly, relaunching.  Some resources might leak.
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=38737, ip=10.0.97.152)[0m [1;36m(Worker pid=38837)[0;0m   warnings.warn('resource_tracker: process died unexpectedly, '
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36255, ip=10.0.125.65)[0m Traceback (most recent call last):
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36255, ip=10.0.125.65)[0m   File "/home/ray/anaconda3/lib/python3.11/multiprocessing/resource_tracker.py", line 239, in main
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36255, ip=10.0.125.65)[0m     cache[rtype].remove(name)
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36255, ip=10.0.125.65)[0m KeyError: '/psm_63feb773'
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m Max pending requests is set to 141
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m Downloading model and tokenizer.
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m No cloud storage mirror configured


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m INFO 11-13 01:26:37 [__init__.py:216] Automatically detected platform cuda.
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=39835, ip=10.0.78.242)[0m [1;36m(Worker pid=39930)[0;0m INFO 11-13 01:26:32 [multiproc_executor.py:558] Parent process exited, terminating worker[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=39835, ip=10.0.78.242)[0m [1;36m(Worker pid=39930)[0;0m INFO 11-13 01:26:32 [multiproc_executor.py:599] WorkerProc shutting down.[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m INFO 11-13 01:26:39 [model.py:547] Resolved architecture: Qwen2_5_VLForConditionalGeneration
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m INFO 11-13 01:26:39 [model.py:1510] Using max model len 8192
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m INFO 11-13 01:26:39 [arg_utils.py:1215] Using ray runtime env: {'_ray_commit': 'aceca04cbbf42e2c50a7e523108a35961c1a612e', 'cgroupv2': {}, 'ray_debugger': {'working_dir': '/home/ray/default'}, 'working_dir': 'gcs://_ray_pkg_71ce8c148e2944680e6a5fd1071541790989e709.zip', 'pip': {'packages': ['datasets'], 'pip_check': False}, 'env_vars': {'LARGE_DATASET_LIMIT': '10000'}}
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m INFO 11-13 01:26:39 [scheduler.py:205] Chunked prefill is enabled with max_num_batched_tokens=2048.
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m WARNING 11-13 01:26:42 [__init__.py:3036] We must use the `spawn` multiprocessing start method. Overriding VLLM_WORKER_MULTIPROC_METHOD to 'spawn'. See https://docs.vllm.ai/en/latest/usage/troubleshooting.html#python-multiprocessing for more information. Reasons: In a Ray actor and can only be spawned
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=51380, ip=10.0.119.39)[0m INFO 11-13 01:26:38 [__init__.py:216] Automatically detected platform cuda.
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m INFO 11-13 01:26:46 [__init__.py:216] Automatically detected platform cuda.
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=51380, ip=10.0.119.39)[0m INFO 11-13 01:26:40 [model.py:547] Resolved architecture: Qwen2_5_VLForConditionalGeneration[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=51380, ip=10.0.119.39)[0m INFO 11-13 01:26:40 [model.py:1510] Using max model len 8192[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=51380, ip=10.0.119.39)[0m INFO 11-13 01:26:40 [arg_utils.py:1215] Using ray runtime env: {'_ray_commit': 'aceca04cbbf42e2c50a7e523108a35961c1a612e', 'cgroupv2': {}, 'ray_debugger': {'working_dir': '/home/ray/default'}, 'working_dir': 'gcs://_ray_pkg_71ce8c148e2944680e6a5fd1071541790989e709.zip', 'pip': {'packages': ['datasets'], 'pip_check': False}, 'env_vars': {'LARGE_DATASET_LIMIT': '10000'}}[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=51380, ip=10.0.119.39)[0m INFO 11-13 01:26:40 [scheduler.py:205] Chunked prefill is enabled with max_num_batched_tokens=2048.[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m [1;36m(EngineCore_DP0 pid=44446)[0;0m INFO 11-13 01:26:47 [core.py:644] Waiting for init message from front-end.
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m [1;36m(EngineCore_DP0 pid=44446)[0;0m INFO 11-13 01:26:47 [core.py:77] Initializing a V1 LLM engine (v0.11.0) with config: model='Qwen/Qwen2.5-VL-3B-Instruct', speculative_config=None, tokenizer='Qwen/Qwen2.5-VL-3B-Instruct', skip_tokenizer_init=False, tokenizer_mode=auto, revision=None, tokenizer_revision=None, trust_remote_code=False, dtype=torch.bfloat16, max_seq_len=8192, download_dir=None, load_format=auto, tensor_parallel_size=1, pipeline_parallel_size=1, data_parallel_size=1, disable_custom_all_reduce=False, quantization=None, enforce_eager=False, kv_cache_dtype=auto, device_config=cuda, structured_outputs_config=StructuredOutputsConfig(backend='auto', disable_fallback=False, disable_any_whitespace=False, disable_additional_properties=False, reasoning_parser=''), observability_config=ObservabilityConfig(show_hidden_metrics_for_version=None, otlp_traces_endpoint=None, collect_detailed_traces=None), seed=0, served_model_name=Qwen/Qwen2.5-VL-3B-Instruct, enable_prefix_caching=True, chunked_prefill_enabled=True, pooler_config=None, compilation_config={"level":3,"debug_dump_path":"","cache_dir":"","backend":"","custom_ops":[],"splitting_ops":["vllm.unified_attention","vllm.unified_attention_with_output","vllm.mamba_mixer2","vllm.mamba_mixer","vllm.short_conv","vllm.linear_attention","vllm.plamo2_mamba_mixer","vllm.gdn_attention","vllm.sparse_attn_indexer"],"use_inductor":true,"compile_sizes":[],"inductor_compile_config":{"enable_auto_functionalized_v2":false},"inductor_passes":{},"cudagraph_mode":[2,1],"use_cudagraph":true,"cudagraph_num_of_warmups":1,"cudagraph_capture_sizes":[256,248,240,232,224,216,208,200,192,184,176,168,160,152,144,136,128,120,112,104,96,88,80,72,64,56,48,40,32,24,16,8,4,2,1],"cudagraph_copy_inputs":false,"full_cuda_graph":false,"use_inductor_graph_partition":false,"pass_config":{},"max_capture_size":256,"local_cache_dir":null}
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m [1;36m(EngineCore_DP0 pid=44446)[0;0m INFO 11-13 01:26:47 [shm_broadcast.py:289] vLLM message queue communication handle: Handle(local_reader_ranks=[0], buffer_handle=(1, 16777216, 10, 'psm_a5f71812'), local_subscribe_addr='ipc:///tmp/32fcb539-f8a7-4fc1-8242-b1088a9095d6', remote_subscribe_addr=None, remote_addr_ipv6=False)
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=51380, ip=10.0.119.39)[0m WARNING 11-13 01:26:43 [__init__.py:3036] We must use the `spawn` multiprocessing start method. Overriding VLLM_WORKER_MULTIPROC_METHOD to 'spawn'. See https://docs.vllm.ai/en/latest/usage/troubleshooting.html#python-multiprocessing for more information. Reasons: In a Ray actor and can only be spawned
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=51380, ip=10.0.119.39)[0m INFO 11-13 01:26:50 [__init__.py:216] Automatically detected platform cuda.[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=51380, ip=10.0.119.39)[0m [1;36m(EngineCore_DP0 pid=51483)[0;0m INFO 11-13 01:26:47 [core.py:644] Waiting for init message from front-end.
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=51380, ip=10.0.119.39)[0m [1;36m(EngineCore_DP0 pid=51483)[0;0m INFO 11-13 01:26:47 [core.py:77] Initializing a V1 LLM engine (v0.11.0) with config: model='Qwen/Qwen2.5-VL-3B-Instruct', speculative_config=None, tokenizer='Qwen/Qwen2.5-VL-3B-Instruct', skip_tokenizer_init=False, tokenizer_mode=auto, revision=None, tokenizer_revision=None, trust_remote_code=False, dtype=torch.bfloat16, max_seq_len=8192, download_dir=None, load_format=auto, tensor_parallel_size=1, pipeline_parallel_size=1, data_parallel_size=1, disable_custom_all_reduce=False, quantization=None, enforce_eager=False, kv_cache_dtype=auto, device_config=cuda, structured_outputs_config=StructuredOutputsConfig(backend='auto', disable_fallback=False, disable_any_whitespace=False, disable_additional_properties=False, reasoning_parser=''), observability_config=ObservabilityConfig(show_hidden_metrics_for_version=None, otlp_traces_endpoint=None, collect_detailed_traces=None), seed=0, served_model_name=Qwen/Qwen2.5-VL-3B-Instruct, enable_prefix_caching=True, chunked_prefill_enabled=True, pooler_config=None, compilation_config={"level":3,"debug_dump_path":"","cache_dir":"","backend":"","custom_ops":[],"splitting_ops":["vllm.unified_attention","vllm.unified_attention_with_output","vllm.mamba_mixer2","vllm.mamba_mixer","vllm.short_conv","vllm.linear_attention","vllm.plamo2_mamba_mixer","vllm.gdn_attention","vllm.sparse_attn_indexer"],"use_inductor":true,"compile_sizes":[],"inductor_compile_config":{"enable_auto_functionalized_v2":false},"inductor_passes":{},"cudagraph_mode":[2,1],"use_cudagraph":true,"cudagraph_num_of_warmups":1,"cudagraph_capture_sizes":[256,248,240,232,224,216,208,200,192,184,176,168,160,152,144,136,128,120,112,104,96,88,80,72,64,56,48,40,32,24,16,8,4,2,1],"cudagraph_copy_inputs":false,"full_cuda_graph":false,"use_inductor_graph_partition":false,"pass_config":{},"max_capture_size":256,"local_cache_dir":null}
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m INFO 11-13 01:26:53 [shm_broadcast.py:289] vLLM message queue communication handle: Handle(local_reader_ranks=[0], buffer_handle=(1, 10485760, 10, 'psm_6068611c'), local_subscribe_addr='ipc:///tmp/1dbe8d51-4ee1-4697-86c0-c6fc81f1ab4b', remote_subscribe_addr=None, remote_addr_ipv6=False)[32m [repeated 2x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m [Gloo] Rank 0 is connected to 0 peer ranks. Expected number of connected peer ranks is : 0
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m [Gloo] Rank 0 is connected to 0 peer ranks. Expected number of connected peer ranks is : 0
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m [Gloo] Rank 0 is connected to 0 peer ranks. Expected number of connected peer ranks is : 0
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m [Gloo] Rank 0 is connected to 0 peer ranks. Expected number of connected peer ranks is : 0
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m [Gloo] Rank 0 is connected to 0 peer ranks. Expected number of connected peer ranks is : 0
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m [Gloo] Rank 0 is connected to 0 peer ranks. Expected number of connected peer ranks is : 0
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m INFO 11-13 01:26:54 [parallel_state.py:1208] rank 0 in world size 1 is assigned as DP rank 0, PP rank 0, TP rank 0, EP rank 0
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m WARNING 11-13 01:26:54 [topk_topp_sampler.py:66] FlashInfer is not available. Falling back to the PyTorch-native implementation of top-p & top-k sampling. For the best performance, please install FlashInfer.


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m You have video processor config saved in `preprocessor.json` file which is deprecated. Video processor configs should be saved in their own `video_preprocessor.json` file. You can rename the file or load and save the processor back which renames it automatically. Loading from `preprocessor.json` will be removed in v5.0.
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=39835, ip=10.0.78.242)[0m [1;36m(Worker pid=39930)[0;0m /home/ray/anaconda3/lib/python3.11/multiprocessing/resource_tracker.py:123: UserWarning: resource_tracker: process died unexpectedly, relaunching.  Some resources might leak.[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=39835, ip=10.0.78.242)[0m [1;36m(Worker pid=39930)[0;0m   warnings.warn('resource_tracker: process died unexpectedly, '[32m [repeated 3x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36255, ip=10.0.125.65)[0m Traceback (most recent call last):[32m [repeated 7x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36255, ip=10.0.125.65)[0m   File "/home/ray/anaconda3/lib/python3.11/multiprocessing/resource_tracker.py", line 239, in main[32m [repeated 7x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36255, ip=10.0.125.65)[0m     cache[rtype].remove(name)[32m [repeated 7x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=36255, ip=10.0.125.65)[0m KeyError: '/mp-yshgyi2e'[32m [repeated 7x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=51380, ip=10.0.119.39)[0m Max pending requests is set to 141
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=51380, ip=10.0.119.39)[0m Downloading model and tokenizer.
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=51380, ip=10.0.119.39)[0m No cloud storage mirror configured


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m [1;36m(Worker pid=44481)[0;0m INFO 11-13 01:26:57 [gpu_model_runner.py:2602] Starting to load model Qwen/Qwen2.5-VL-3B-Instruct...
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m [1;36m(Worker pid=44481)[0;0m INFO 11-13 01:26:57 [gpu_model_runner.py:2634] Loading model from scratch...
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m [1;36m(Worker pid=44481)[0;0m INFO 11-13 01:26:57 [cuda.py:366] Using Flash Attention backend on V1 engine.
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m [1;36m(Worker pid=44481)[0;0m INFO 11-13 01:26:57 [weight_utils.py:392] Using model weights format ['*.safetensors']


    Loading safetensors checkpoint shards:   0% Completed | 0/2 [00:00<?, ?it/s]0m [1;36m(Worker pid=44481)[0;0m 
    Loading safetensors checkpoint shards:  50% Completed | 1/2 [00:00<00:00,  2.18it/s]6m(Worker pid=44481)[0;0m 
    Loading safetensors checkpoint shards: 100% Completed | 2/2 [00:01<00:00,  1.74it/s]6m(Worker pid=44481)[0;0m 
    Loading safetensors checkpoint shards: 100% Completed | 2/2 [00:01<00:00,  1.79it/s]6m(Worker pid=44481)[0;0m 
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m [1;36m(Worker pid=44481)[0;0m 


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m [1;36m(Worker pid=44481)[0;0m INFO 11-13 01:26:59 [default_loader.py:267] Loading weights took 1.25 seconds
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=51380, ip=10.0.119.39)[0m INFO 11-13 01:26:54 [shm_broadcast.py:289] vLLM message queue communication handle: Handle(local_reader_ranks=[0], buffer_handle=(1, 10485760, 10, 'psm_60664aa2'), local_subscribe_addr='ipc:///tmp/94765b0e-f850-48eb-8c8c-6f42a396c03b', remote_subscribe_addr=None, remote_addr_ipv6=False)
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m [1;36m(Worker pid=44481)[0;0m INFO 11-13 01:26:59 [gpu_model_runner.py:2653] Model loading took 7.1550 GiB and 1.798642 seconds
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=51380, ip=10.0.119.39)[0m [Gloo] Rank 0 is connected to 0 peer ranks. Expected number of connected peer ranks is : 0[32m [repeated 6x across cluster][0m
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=51380, ip=10.0.119.39)[0m INFO 11-13 01:26:55 [parallel_state.py:1208] rank 0 in world size 1 is assigned as DP rank 0, PP rank 0, TP rank 0, EP rank 0
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m [1;36m(Worker pid=44481)[0;0m INFO 11-13 01:27:00 [gpu_model_runner.py:3344] Encoder cache will be initialized with a budget of 16384 tokens, and profiled with 1 image items of the maximum feature size.
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=51380, ip=10.0.119.39)[0m WARNING 11-13 01:26:55 [topk_topp_sampler.py:66] FlashInfer is not available. Falling back to the PyTorch-native implementation of top-p & top-k sampling. For the best performance, please install FlashInfer.


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=51380, ip=10.0.119.39)[0m [1;36m(Worker pid=51527)[0;0m 


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m [1;36m(Worker pid=44481)[0;0m INFO 11-13 01:27:13 [backends.py:548] Using cache directory: /home/ray/.cache/vllm/torch_compile_cache/a314dc5b3a/rank_0_0/backbone for vLLM's torch.compile
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m [1;36m(Worker pid=44481)[0;0m INFO 11-13 01:27:13 [backends.py:559] Dynamo bytecode transform time: 6.69 s
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=51380, ip=10.0.119.39)[0m [1;36m(Worker pid=51527)[0;0m INFO 11-13 01:26:58 [gpu_model_runner.py:2602] Starting to load model Qwen/Qwen2.5-VL-3B-Instruct...
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=51380, ip=10.0.119.39)[0m [1;36m(Worker pid=51527)[0;0m INFO 11-13 01:26:58 [gpu_model_runner.py:2634] Loading model from scratch...
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=51380, ip=10.0.119.39)[0m [1;36m(Worker pid=51527)[0;0m INFO 11-13 01:26:58 [cuda.py:366] Using Flash Attention backend on V1 engine.
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=51380, ip=10.0.119.39)[0m [1;36m(Worker pid=51527)[0;0m INFO 11-13 01:26:58 [weight_utils.py:392] Using model weights format ['*.safetensors']
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=51380, ip=10.0.119.39)[0m [1;36m(Worker pid=51527)[0;0m INFO 11-13 01:27:00 [default_loader.py:267] Loading weights took 1.36 seconds
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=51380, ip=10.0.119.39)[0m [1;36m(Worker pid=51527)[0;0m INFO 11-13 01:27:00 [gpu_model_runner.py:2653] Model loading took 7.1550 GiB and 1.903490 seconds
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=51380, ip=10.0.119.39)[0m [1;36m(Worker pid=51527)[0;0m INFO 11-13 01:27:00 [gpu_model_runner.py:3344] Encoder cache will be initialized with a budget of 16384 tokens, and profiled with 1 image items of the maximum feature size.
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m [1;36m(Worker pid=44481)[0;0m INFO 11-13 01:27:15 [backends.py:164] Directly load the compiled graph(s) for dynamic shape from the cache, took 1.673 s
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m [1;36m(Worker pid=44481)[0;0m INFO 11-13 01:27:16 [monitor.py:34] torch.compile takes 6.69 s in total
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m [1;36m(Worker pid=44481)[0;0m INFO 11-13 01:27:17 [gpu_worker.py:298] Available KV cache memory: 9.26 GiB
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m [1;36m(EngineCore_DP0 pid=44446)[0;0m INFO 11-13 01:27:17 [kv_cache_utils.py:1087] GPU KV cache size: 269,568 tokens
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m [1;36m(EngineCore_DP0 pid=44446)[0;0m INFO 11-13 01:27:17 [kv_cache_utils.py:1091] Maximum concurrency for 8,192 tokens per request: 32.91x


    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):   0%|          | 0/35 [00:00<?, ?it/s]id=44481)[0;0m 
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=51380, ip=10.0.119.39)[0m You have video processor config saved in `preprocessor.json` file which is deprecated. Video processor configs should be saved in their own `video_preprocessor.json` file. You can rename the file or load and save the processor back which renames it automatically. Loading from `preprocessor.json` will be removed in v5.0.
    Loading safetensors checkpoint shards:   0% Completed | 0/2 [00:00<?, ?it/s]0m [1;36m(Worker pid=51527)[0;0m 
    Loading safetensors checkpoint shards: 100% Completed | 2/2 [00:01<00:00,  1.63it/s][32m [repeated 3x across cluster][0m
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):   6%|▌         | 2/35 [00:00<00:02, 13.56it/s]
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  11%|█▏        | 4/35 [00:00<00:02, 14.41it/s]
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  17%|█▋        | 6/35 [00:00<00:01, 14.81it/s]
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  23%|██▎       | 8/35 [00:00<00:01, 14.93it/s]
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  29%|██▊       | 10/35 [00:00<00:01, 14.98it/s]
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  34%|███▍      | 12/35 [00:00<00:01, 15.20it/s]
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  91%|█████████▏| 32/35 [00:02<00:00, 16.32it/s]
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE): 100%|██████████| 35/35 [00:02<00:00, 15.51it/s]
    Capturing CUDA graphs (decode, FULL):   0%|          | 0/19 [00:00<?, ?it/s]0m [1;36m(Worker pid=44481)[0;0m 
    Capturing CUDA graphs (decode, FULL):  11%|█         | 2/19 [00:00<00:01, 16.09it/s]
    Capturing CUDA graphs (decode, FULL):  21%|██        | 4/19 [00:00<00:00, 16.77it/s]
    Capturing CUDA graphs (decode, FULL):  32%|███▏      | 6/19 [00:00<00:00, 16.98it/s]
    Capturing CUDA graphs (decode, FULL):  42%|████▏     | 8/19 [00:00<00:00, 17.29it/s]
    Capturing CUDA graphs (decode, FULL):  53%|█████▎    | 10/19 [00:00<00:00, 17.79it/s]
    Capturing CUDA graphs (decode, FULL):  63%|██████▎   | 12/19 [00:00<00:00, 17.97it/s]
    Capturing CUDA graphs (decode, FULL):  74%|███████▎  | 14/19 [00:00<00:00, 18.13it/s]
    Capturing CUDA graphs (decode, FULL):  84%|████████▍ | 16/19 [00:00<00:00, 18.26it/s]
    Capturing CUDA graphs (decode, FULL):  95%|█████████▍| 18/19 [00:01<00:00, 18.50it/s]
    Capturing CUDA graphs (decode, FULL): 100%|██████████| 19/19 [00:01<00:00, 18.02it/s]


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m [1;36m(Worker pid=44481)[0;0m INFO 11-13 01:27:21 [gpu_model_runner.py:3480] Graph capturing finished in 4 secs, took 0.44 GiB
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m [1;36m(EngineCore_DP0 pid=44446)[0;0m INFO 11-13 01:27:21 [core.py:210] init engine (profile, create kv cache, warmup model) took 21.72 seconds
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=51380, ip=10.0.119.39)[0m [1;36m(Worker pid=51527)[0;0m INFO 11-13 01:27:14 [backends.py:548] Using cache directory: /home/ray/.cache/vllm/torch_compile_cache/a314dc5b3a/rank_0_0/backbone for vLLM's torch.compile
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=51380, ip=10.0.119.39)[0m [1;36m(Worker pid=51527)[0;0m INFO 11-13 01:27:14 [backends.py:559] Dynamo bytecode transform time: 6.44 s
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=51380, ip=10.0.119.39)[0m [1;36m(Worker pid=51527)[0;0m INFO 11-13 01:27:16 [backends.py:164] Directly load the compiled graph(s) for dynamic shape from the cache, took 1.712 s
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=51380, ip=10.0.119.39)[0m [1;36m(Worker pid=51527)[0;0m INFO 11-13 01:27:17 [monitor.py:34] torch.compile takes 6.44 s in total
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=51380, ip=10.0.119.39)[0m [1;36m(Worker pid=51527)[0;0m INFO 11-13 01:27:18 [gpu_worker.py:298] Available KV cache memory: 9.26 GiB


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m [1;36m(EngineCore_DP0 pid=44446)[0;0m You have video processor config saved in `preprocessor.json` file which is deprecated. Video processor configs should be saved in their own `video_preprocessor.json` file. You can rename the file or load and save the processor back which renames it automatically. Loading from `preprocessor.json` will be removed in v5.0.
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):   0%|          | 0/35 [00:00<?, ?it/s]id=51527)[0;0m 
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE):  86%|████████▌ | 30/35 [00:02<00:00, 15.42it/s][32m [repeated 24x across cluster][0m
    Capturing CUDA graphs (mixed prefill-decode, PIECEWISE): 100%|██████████| 35/35 [00:02<00:00, 14.89it/s][32m [repeated 3x across cluster][0m
    Capturing CUDA graphs (decode, FULL):   0%|          | 0/19 [00:00<?, ?it/s]0m [1;36m(Worker pid=51527)[0;0m 
    Capturing CUDA graphs (decode, FULL):  84%|████████▍ | 16/19 [00:00<00:00, 17.32it/s][32m [repeated 8x across cluster][0m
    Capturing CUDA graphs (decode, FULL): 100%|██████████| 19/19 [00:01<00:00, 16.85it/s][32m [repeated 2x across cluster][0m


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m INFO 11-13 01:27:26 [loggers.py:147] Engine 000: vllm cache_config_info with initialization after num_gpu_blocks is: 16848
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=51380, ip=10.0.119.39)[0m [1;36m(EngineCore_DP0 pid=51483)[0;0m INFO 11-13 01:27:18 [kv_cache_utils.py:1087] GPU KV cache size: 269,568 tokens
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=51380, ip=10.0.119.39)[0m [1;36m(EngineCore_DP0 pid=51483)[0;0m INFO 11-13 01:27:18 [kv_cache_utils.py:1091] Maximum concurrency for 8,192 tokens per request: 32.91x
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=51380, ip=10.0.119.39)[0m [1;36m(Worker pid=51527)[0;0m INFO 11-13 01:27:22 [gpu_model_runner.py:3480] Graph capturing finished in 4 secs, took 0.44 GiB
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=51380, ip=10.0.119.39)[0m [1;36m(EngineCore_DP0 pid=51483)[0;0m INFO 11-13 01:27:22 [core.py:210] init engine (profile, create kv cache, warmup model) took 21.88 seconds


    2025-11-13 01:29:42,963	ERROR streaming_executor_state.py:618 -- An exception was raised from a task of operator "ReadHuggingFace->SplitBlocks(200)". Dataset execution will now abort. To ignore this exception and continue, set DataContext.max_errored_blocks.
    Traceback (most recent call last):
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/execution/streaming_executor_state.py", line 586, in process_completed_tasks
        bytes_read = task.on_data_ready(
                     ^^^^^^^^^^^^^^^^^^^
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/execution/interfaces/physical_operator.py", line 199, in on_data_ready
        raise ex from None
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/execution/interfaces/physical_operator.py", line 194, in on_data_ready
        ray.get(self._pending_block_ref)
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/_private/auto_init_hook.py", line 22, in auto_init_wrapper
        return fn(*args, **kwargs)
               ^^^^^^^^^^^^^^^^^^^
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/_private/client_mode_hook.py", line 104, in wrapper
        return func(*args, **kwargs)
               ^^^^^^^^^^^^^^^^^^^^^
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/_private/worker.py", line 2961, in get
        values, debugger_breakpoint = worker.get_objects(
                                      ^^^^^^^^^^^^^^^^^^^
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/_private/worker.py", line 1026, in get_objects
        raise value.as_instanceof_cause()
    ray.exceptions.RayTaskError(ArrowInvalid): [36mray::ReadHuggingFace->SplitBlocks(200)()[39m (pid=28715, ip=10.0.119.39)
        for b_out in map_transformer.apply_transform(iter(blocks), ctx):
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/execution/operators/map_transformer.py", line 102, in __call__
        yield from self._post_process(results)
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/execution/operators/map_transformer.py", line 399, in _apply_transform
        yield from self._block_fn(blocks, ctx)
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/execution/operators/map_operator.py", line 822, in _split_blocks
        for block in blocks:
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/execution/operators/map_transformer.py", line 102, in __call__
        yield from self._post_process(results)
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/execution/operators/map_transformer.py", line 84, in _shape_blocks
        for result in results:
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/execution/operators/map_transformer.py", line 399, in _apply_transform
        yield from self._block_fn(blocks, ctx)
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/planner/plan_read_op.py", line 107, in do_read
        yield from read_task()
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/datasource/datasource.py", line 222, in __call__
        yield from result
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/datasource/huggingface_datasource.py", line 137, in _read_dataset
        for batch in self._dataset.with_format("arrow").iter(
      File "/tmp/ray/session_2025-11-12_14-59-56_310597_2440/runtime_resources/pip/e8b52151188f6d950bae9c3596c35e6295e8ffb9/virtualenv/lib/python3.11/site-packages/datasets/iterable_dataset.py", line 2563, in iter
        for key, pa_table in iterator:
      File "/tmp/ray/session_2025-11-12_14-59-56_310597_2440/runtime_resources/pip/e8b52151188f6d950bae9c3596c35e6295e8ffb9/virtualenv/lib/python3.11/site-packages/datasets/iterable_dataset.py", line 2078, in _iter_arrow
        for key, pa_table in self.ex_iterable._iter_arrow():
      File "/tmp/ray/session_2025-11-12_14-59-56_310597_2440/runtime_resources/pip/e8b52151188f6d950bae9c3596c35e6295e8ffb9/virtualenv/lib/python3.11/site-packages/datasets/iterable_dataset.py", line 567, in _iter_arrow
        yield new_key, pa.Table.from_batches(chunks_buffer)
                       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
      File "pyarrow/table.pxi", line 5039, in pyarrow.lib.Table.from_batches
      File "pyarrow/error.pxi", line 155, in pyarrow.lib.pyarrow_internal_check_status
      File "pyarrow/error.pxi", line 92, in pyarrow.lib.check_status
    pyarrow.lib.ArrowInvalid: Schema at index 1695 was different: 
    caption.txt: string
    image.png: struct<bytes: binary, path: string>
    __key__: string
    __url__: string
    vs
    __key__: string
    __url__: string
    caption.txt: string
    image.png: struct<bytes: binary, path: string>
    2025-11-13 01:29:43,029	INFO streaming_executor.py:298 -- ⚠️  Dataset dataset_247_0 execution failed
    2025-11-13 01:29:43,054	ERROR exceptions.py:73 -- Exception occurred in Ray Data or Ray Core internal code. If you continue to see this error, please open an issue on the Ray project GitHub page with the full stack trace below: https://github.com/ray-project/ray/issues/new/choose
    2025-11-13 01:29:43,055	ERROR exceptions.py:81 -- Full stack trace:
    Traceback (most recent call last):
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/exceptions.py", line 49, in handle_trace
        return fn(*args, **kwargs)
               ^^^^^^^^^^^^^^^^^^^
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/plan.py", line 533, in execute
        blocks = execute_to_legacy_block_list(
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/execution/legacy_compat.py", line 127, in execute_to_legacy_block_list
        block_list = _bundles_to_block_list(bundles)
                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/execution/legacy_compat.py", line 175, in _bundles_to_block_list
        bundle_list = list(bundles)
                      ^^^^^^^^^^^^^
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/execution/interfaces/executor.py", line 34, in __next__
        return self.get_next()
               ^^^^^^^^^^^^^^^
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/execution/streaming_executor.py", line 730, in get_next
        bundle = state.get_output_blocking(output_split_idx)
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/execution/streaming_executor_state.py", line 443, in get_output_blocking
        raise self._exception
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/execution/streaming_executor.py", line 354, in run
        continue_sched = self._scheduling_loop_step(self._topology)
                         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/execution/streaming_executor.py", line 459, in _scheduling_loop_step
        num_errored_blocks = process_completed_tasks(
                             ^^^^^^^^^^^^^^^^^^^^^^^^
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/execution/streaming_executor_state.py", line 619, in process_completed_tasks
        raise e from None
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/execution/streaming_executor_state.py", line 586, in process_completed_tasks
        bytes_read = task.on_data_ready(
                     ^^^^^^^^^^^^^^^^^^^
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/execution/interfaces/physical_operator.py", line 199, in on_data_ready
        raise ex from None
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/execution/interfaces/physical_operator.py", line 194, in on_data_ready
        ray.get(self._pending_block_ref)
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/_private/auto_init_hook.py", line 22, in auto_init_wrapper
        return fn(*args, **kwargs)
               ^^^^^^^^^^^^^^^^^^^
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/_private/client_mode_hook.py", line 104, in wrapper
        return func(*args, **kwargs)
               ^^^^^^^^^^^^^^^^^^^^^
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/_private/worker.py", line 2961, in get
        values, debugger_breakpoint = worker.get_objects(
                                      ^^^^^^^^^^^^^^^^^^^
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/_private/worker.py", line 1026, in get_objects
        raise value.as_instanceof_cause()
    ray.exceptions.RayTaskError(ArrowInvalid): [36mray::ReadHuggingFace->SplitBlocks(200)()[39m (pid=28715, ip=10.0.119.39)
        for b_out in map_transformer.apply_transform(iter(blocks), ctx):
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/execution/operators/map_transformer.py", line 102, in __call__
        yield from self._post_process(results)
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/execution/operators/map_transformer.py", line 399, in _apply_transform
        yield from self._block_fn(blocks, ctx)
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/execution/operators/map_operator.py", line 822, in _split_blocks
        for block in blocks:
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/execution/operators/map_transformer.py", line 102, in __call__
        yield from self._post_process(results)
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/execution/operators/map_transformer.py", line 84, in _shape_blocks
        for result in results:
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/execution/operators/map_transformer.py", line 399, in _apply_transform
        yield from self._block_fn(blocks, ctx)
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/planner/plan_read_op.py", line 107, in do_read
        yield from read_task()
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/datasource/datasource.py", line 222, in __call__
        yield from result
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/datasource/huggingface_datasource.py", line 137, in _read_dataset
        for batch in self._dataset.with_format("arrow").iter(
      File "/tmp/ray/session_2025-11-12_14-59-56_310597_2440/runtime_resources/pip/e8b52151188f6d950bae9c3596c35e6295e8ffb9/virtualenv/lib/python3.11/site-packages/datasets/iterable_dataset.py", line 2563, in iter
        for key, pa_table in iterator:
      File "/tmp/ray/session_2025-11-12_14-59-56_310597_2440/runtime_resources/pip/e8b52151188f6d950bae9c3596c35e6295e8ffb9/virtualenv/lib/python3.11/site-packages/datasets/iterable_dataset.py", line 2078, in _iter_arrow
        for key, pa_table in self.ex_iterable._iter_arrow():
      File "/tmp/ray/session_2025-11-12_14-59-56_310597_2440/runtime_resources/pip/e8b52151188f6d950bae9c3596c35e6295e8ffb9/virtualenv/lib/python3.11/site-packages/datasets/iterable_dataset.py", line 567, in _iter_arrow
        yield new_key, pa.Table.from_batches(chunks_buffer)
                       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
      File "pyarrow/table.pxi", line 5039, in pyarrow.lib.Table.from_batches
      File "pyarrow/error.pxi", line 155, in pyarrow.lib.pyarrow_internal_check_status
      File "pyarrow/error.pxi", line 92, in pyarrow.lib.check_status
    pyarrow.lib.ArrowInvalid: Schema at index 1695 was different: 
    caption.txt: string
    image.png: struct<bytes: binary, path: string>
    __key__: string
    __url__: string
    vs
    __key__: string
    __url__: string
    caption.txt: string
    image.png: struct<bytes: binary, path: string>



    ---------------------------------------------------------------------------

    SystemException                           Traceback (most recent call last)

    SystemException: 

    
    The above exception was the direct cause of the following exception:


    RayTaskError(ArrowInvalid)                Traceback (most recent call last)

    /home/ray/default/llm_batch_inference_vision.ipynb Cell 19 line 2
         <a href='vscode-notebook-cell://vscode-session-zqxtzm1ybdul1exnjmekvr4rru.i.anyscaleuserdata.com/home/ray/default/llm_batch_inference_vision.ipynb#Y112sdnNjb2RlLXJlbW90ZQ%3D%3D?line=24'>25</a> # Run the same processor on the larger dataset.
         <a href='vscode-notebook-cell://vscode-session-zqxtzm1ybdul1exnjmekvr4rru.i.anyscaleuserdata.com/home/ray/default/llm_batch_inference_vision.ipynb#Y112sdnNjb2RlLXJlbW90ZQ%3D%3D?line=25'>26</a> processed_large = processor(ds_large)
    ---> <a href='vscode-notebook-cell://vscode-session-zqxtzm1ybdul1exnjmekvr4rru.i.anyscaleuserdata.com/home/ray/default/llm_batch_inference_vision.ipynb#Y112sdnNjb2RlLXJlbW90ZQ%3D%3D?line=26'>27</a> processed_large = processed_large.materialize()
         <a href='vscode-notebook-cell://vscode-session-zqxtzm1ybdul1exnjmekvr4rru.i.anyscaleuserdata.com/home/ray/default/llm_batch_inference_vision.ipynb#Y112sdnNjb2RlLXJlbW90ZQ%3D%3D?line=28'>29</a> print(f"\nProcessed {processed_large.count()} images successfully.")
         <a href='vscode-notebook-cell://vscode-session-zqxtzm1ybdul1exnjmekvr4rru.i.anyscaleuserdata.com/home/ray/default/llm_batch_inference_vision.ipynb#Y112sdnNjb2RlLXJlbW90ZQ%3D%3D?line=29'>30</a> print("\nSample outputs:")


    File ~/anaconda3/lib/python3.11/site-packages/ray/data/dataset.py:6010, in Dataset.materialize(self)
       5990 """Execute and materialize this dataset into object store memory.
       5991 
       5992 This can be used to read all blocks into memory. By default, Dataset
       (...)
       6006     A MaterializedDataset holding the materialized data blocks.
       6007 """
       6008 copy = Dataset.copy(self, _deep_copy=True, _as=MaterializedDataset)
    -> 6010 bundle: RefBundle = copy._plan.execute()
       6011 blocks_with_metadata = bundle.blocks
       6013 # TODO(hchen): Here we generate the same number of blocks as
       6014 # the original Dataset. Because the old code path does this, and
       6015 # some unit tests implicily depend on this behavior.
       6016 # After we remove the old code path, we should consider merging
       6017 # some blocks for better perf.


    File ~/anaconda3/lib/python3.11/site-packages/ray/data/exceptions.py:89, in omit_traceback_stdout.<locals>.handle_trace(*args, **kwargs)
         87     raise e.with_traceback(None)
         88 else:
    ---> 89     raise e.with_traceback(None) from SystemException()


    RayTaskError(ArrowInvalid): ray::ReadHuggingFace->SplitBlocks(200)() (pid=28715, ip=10.0.119.39)
        for b_out in map_transformer.apply_transform(iter(blocks), ctx):
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/execution/operators/map_transformer.py", line 102, in __call__
        yield from self._post_process(results)
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/execution/operators/map_transformer.py", line 399, in _apply_transform
        yield from self._block_fn(blocks, ctx)
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/execution/operators/map_operator.py", line 822, in _split_blocks
        for block in blocks:
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/execution/operators/map_transformer.py", line 102, in __call__
        yield from self._post_process(results)
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/execution/operators/map_transformer.py", line 84, in _shape_blocks
        for result in results:
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/execution/operators/map_transformer.py", line 399, in _apply_transform
        yield from self._block_fn(blocks, ctx)
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/planner/plan_read_op.py", line 107, in do_read
        yield from read_task()
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/datasource/datasource.py", line 222, in __call__
        yield from result
      File "/home/ray/anaconda3/lib/python3.11/site-packages/ray/data/_internal/datasource/huggingface_datasource.py", line 137, in _read_dataset
        for batch in self._dataset.with_format("arrow").iter(
      File "/tmp/ray/session_2025-11-12_14-59-56_310597_2440/runtime_resources/pip/e8b52151188f6d950bae9c3596c35e6295e8ffb9/virtualenv/lib/python3.11/site-packages/datasets/iterable_dataset.py", line 2563, in iter
        for key, pa_table in iterator:
      File "/tmp/ray/session_2025-11-12_14-59-56_310597_2440/runtime_resources/pip/e8b52151188f6d950bae9c3596c35e6295e8ffb9/virtualenv/lib/python3.11/site-packages/datasets/iterable_dataset.py", line 2078, in _iter_arrow
        for key, pa_table in self.ex_iterable._iter_arrow():
      File "/tmp/ray/session_2025-11-12_14-59-56_310597_2440/runtime_resources/pip/e8b52151188f6d950bae9c3596c35e6295e8ffb9/virtualenv/lib/python3.11/site-packages/datasets/iterable_dataset.py", line 567, in _iter_arrow
        yield new_key, pa.Table.from_batches(chunks_buffer)
                       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
      File "pyarrow/table.pxi", line 5039, in pyarrow.lib.Table.from_batches
      File "pyarrow/error.pxi", line 155, in pyarrow.lib.pyarrow_internal_check_status
      File "pyarrow/error.pxi", line 92, in pyarrow.lib.check_status
    pyarrow.lib.ArrowInvalid: Schema at index 1695 was different: 
    caption.txt: string
    image.png: struct<bytes: binary, path: string>
    __key__: string
    __url__: string
    vs
    __key__: string
    __url__: string
    caption.txt: string
    image.png: struct<bytes: binary, path: string>


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m [1;36m(Worker pid=43096)[0;0m /home/ray/anaconda3/lib/python3.11/multiprocessing/resource_tracker.py:123: UserWarning: resource_tracker: process died unexpectedly, relaunching.  Some resources might leak.
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m [1;36m(Worker pid=43096)[0;0m   warnings.warn('resource_tracker: process died unexpectedly, '
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m Traceback (most recent call last):
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m   File "/home/ray/anaconda3/lib/python3.11/multiprocessing/resource_tracker.py", line 239, in main
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m     cache[rtype].remove(name)
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m KeyError: '/psm_4c0eea81'
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=44350, ip=10.0.125.65)[0m KeyError: '/mp-hpmlopar'


    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m [1;36m(Worker pid=43096)[0;0m INFO 11-13 01:29:43 [multiproc_executor.py:558] Parent process exited, terminating worker
    [36m(MapWorker(MapBatches(vLLMEngineStageUDF)) pid=42987, ip=10.0.97.54)[0m [1;36m(Worker pid=43096)[0;0m INFO 11-13 01:29:43 [multiproc_executor.py:599] WorkerProc shutting down.


### Performance optimization tips

When scaling to larger datasets, consider these optimizations:

**Adjust concurrency**  
Increase the `concurrency` parameter to add more parallel workers and GPUs.

**Tune batch size**  
For vision models, smaller batch sizes (8-32) often work better due to memory constraints from image processing.

**Optimize image loading**  
Pre-resize images to a consistent size to reduce memory usage and improve throughput.

**Repartition strategically**  
Use more partitions (blocks) than the number of workers to enable better load balancing.

**Enable checkpointing**  
For very large datasets, configure checkpointing to recover from failures:

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
# Save the processed dataset to JSON format (better for text outputs).
# Replace this path with your desired output location.
output_path_small = "local:///tmp/small_processed_captions"
output_path = "local:///tmp/processed_captions"

print(f"Saving small processed dataset to {output_path_small}...")
processed_small.write_json(output_path_small)
print("Saved successfully.")

print(f"Saving large processed dataset to {output_path}...")
processed_large.write_json(output_path)
print("Saved successfully.")

# Alternatively, save as Parquet for better compression:
# processed_small.write_parquet(output_path_small)
# processed_large.write_parquet(output_path)
```

For more information, see [Saving Data](https://docs.ray.io/en/latest/data/saving-data.html).


## Summary

In this notebook, you built an end-to-end vision batch inference pipeline: loading the COCO image dataset into Ray Dataset, configuring a vLLM processor for the Qwen2.5-VL-3B vision-language model, and adding pre/post-processing to generate image captions. You validated the flow on 100,000 images, scaled to 100k images, monitored progress in the Ray Dashboard, and saved the results to persistent storage.

See [Anyscale batch inference optimization](https://docs.anyscale.com/llm/batch-inference) for more information on using Ray Data with Anyscale and for more advanced use cases, see [Working with LLMs](https://docs.ray.io/en/latest/data/working-with-llms.html).

