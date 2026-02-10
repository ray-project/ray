(deployment-initialization-guide)=
# Deployment Initialization

The initialization phase of a serve.llm deployment involves many steps, including preparation of model weights, engine (vLLM) initialization, and Ray serve replica autoscaling overheads. A detailed breakdown of the steps involved in using serve.llm with vLLM is provided below.

## Startup Breakdown
- **Provisioning Nodes**: If a GPU node isn't available, a new instance must be provisioned.
- **Image Download**: Downloading image to target instance incurs latency correlated with image size.
- **Fixed Ray/Node Initialization**: Ray/vLLM incurs some fixed overhead when spawning new processes to handle a new replica, which involves importing large libraries (such as vLLM), preparing model and engine configurations, etc.
- **Model Loading**: Retrieve model either from Hugging Face or cloud storage, including time spent downloading the model and moving it to GPU memory
- **Torch Compile**: Torch compile is integral to vLLM's design and it is enabled by default.
- **Memory Profiling**: vLLM runs some inference on the model to determine the amount of available memory it can dedicate to the KV cache
- **CUDA Graph Capture**: vLLM captures the CUDA graphs for different input sizes ahead of time. More details are [here.](https://docs.vllm.ai/en/latest/design/cuda_graphs.html)
- **Warmup**: Initialize KV cache, run model inference.



This document will provide an overview of the numerous ways to customize your deployment initialization.

## Model Loading from Hugging Face

By default, Ray Serve LLM loads models from Hugging Face Hub. Specify the model source with `model_source`:

```python
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="llama-3-8b",
        model_source="meta-llama/Meta-Llama-3-8B-Instruct",
    ),
    accelerator_type="A10G",
)

app = build_openai_app({"llm_configs": [llm_config]})
serve.run(app, blocking=True)
```

### Load gated models

Gated Hugging Face models require authentication. Pass your Hugging Face token through the `runtime_env`:

```python
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app
import os

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="llama-3-8b-instruct",
        model_source="meta-llama/Meta-Llama-3-8B-Instruct",
    ),
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1,
            max_replicas=2,
        )
    ),
    accelerator_type="A10G",
    runtime_env=dict(
        env_vars={
            "HF_TOKEN": os.environ["HF_TOKEN"]
        }
    ),
)

app = build_openai_app({"llm_configs": [llm_config]})
serve.run(app, blocking=True)
```

You can also set environment variables cluster-wide by passing them to `ray.init`:

```python
import ray

ray.init(
    runtime_env=dict(
        env_vars={
            "HF_TOKEN": os.environ["HF_TOKEN"]
        }
    ),
)
```



### Fast download from Hugging Face

Enable fast downloads with Hugging Face's `hf_transfer` library:

1. Install the library:

```bash
pip install hf_transfer
```

2. Set the `HF_HUB_ENABLE_HF_TRANSFER` environment variable:

```python
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="llama-3-8b",
        model_source="meta-llama/Meta-Llama-3-8B-Instruct",
    ),
    accelerator_type="A10G",
    runtime_env=dict(
        env_vars={
            "HF_HUB_ENABLE_HF_TRANSFER": "1"
        }
    ),
)

app = build_openai_app({"llm_configs": [llm_config]})
serve.run(app, blocking=True)
```


## Model Loading from remote storage

Load models from S3 or GCS buckets instead of Hugging Face. This is useful for:

- Private models not hosted on Hugging Face
- Faster loading from cloud storage in the same region
- Custom model formats or fine-tuned models

### S3 bucket structure

Your S3 bucket should contain the model files in a Hugging Face-compatible structure:

```bash
$ aws s3 ls air-example-data/rayllm-ossci/meta-Llama-3.2-1B-Instruct/
2025-03-25 11:37:48       1519 .gitattributes
2025-03-25 11:37:48       7712 LICENSE.txt
2025-03-25 11:37:48      41742 README.md
2025-03-25 11:37:48       6021 USE_POLICY.md
2025-03-25 11:37:48        877 config.json
2025-03-25 11:37:48        189 generation_config.json
2025-03-25 11:37:48 2471645608 model.safetensors
2025-03-25 11:37:53        296 special_tokens_map.json
2025-03-25 11:37:53    9085657 tokenizer.json
2025-03-25 11:37:53      54528 tokenizer_config.json
```

### Configure S3 loading (YAML)

Use the `bucket_uri` parameter in `model_loading_config`:

```yaml
# config.yaml
applications:
- args:
    llm_configs:
        - accelerator_type: A10G
          engine_kwargs:
            max_model_len: 8192
          model_loading_config:
            model_id: my_llama
            model_source:
              bucket_uri: s3://anonymous@air-example-data/rayllm-ossci/meta-Llama-3.2-1B-Instruct
  import_path: ray.serve.llm:build_openai_app
  name: llm_app
  route_prefix: "/"
```

Deploy with:

```bash
serve deploy config.yaml
```

### Configure S3 loading (Python API)

You can also configure S3 loading with Python:

```python
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="my_llama",
        model_source=dict(
            bucket_uri="s3://my-bucket/path/to/model"
        )
    ),
    accelerator_type="A10G",
    engine_kwargs=dict(
        max_model_len=8192,
    ),
)

app = build_openai_app({"llm_configs": [llm_config]})
serve.run(app, blocking=True)
```

### Configure GCS bucket loading (YAML)

For Google Cloud Storage, use the `gs://` protocol:

```yaml
model_loading_config:
  model_id: my_model
  model_source:
    bucket_uri: gs://my-gcs-bucket/path/to/model
```

### S3 credentials

For private S3 buckets, configure AWS credentials:

1. **Option 1: Environment variables**

```python
llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="my_model",
        model_source=dict(
            bucket_uri="s3://my-private-bucket/model"
        )
    ),
    runtime_env=dict(
        env_vars={
            "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
            "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
        }
    ),
)
```

2. **Option 2: IAM roles** (recommended for production)

Use EC2 instance profiles or EKS service accounts with appropriate S3 read permissions.


### S3 and RunAI Streamer
S3 can be combined with RunAI Streamer, an extension in vLLM that enables streaming the model weights directly from remote cloud storage into GPU memory, improving model load latency. More details can be found [here](https://docs.vllm.ai/en/stable/models/extensions/runai_model_streamer.html).

```python
llm_config = LLMConfig(
    ...
    model_loading_config={
        "model_id": "llama",
        "model_source": "s3://your-bucket/Meta-Llama-3-8B-Instruct",
    },
    engine_kwargs={
        "tensor_parallel_size": 1,
        "load_format": "runai_streamer",
    },
    ...
)
```

### Model Sharding
Modern LLM model sizes often outgrow the memory capacity of a single GPU, requiring the use of tensor parallelism to split computation across multiple devices. In this paradigm, only a subset of weights are stored on each GPU, and model sharding ensures that each device only loads the relevant portion of the model. By sharding the model files in advance, we can reduce load times significantly, since GPUs avoid loading unneeded weights. vLLM provides a utility script for this purpose: [save_sharded_state.py](https://github.com/vllm-project/vllm/blob/main/examples/offline_inference/save_sharded_state.py).

Once the sharded weights have been saved, upload them to S3 and use RunAI streamer with a new flag to load the sharded weights

```python
llm_config = LLMConfig(
    ...
    engine_kwargs={
        "tensor_parallel_size": 4,
        "load_format": "runai_streamer_sharded",
    },
    ...
)
```

## Additional Optimizations

### Torch Compile Cache
Torch.compile incurs some latency during initialization. This can be mitigated by keeping a torch compile cache, which is automatically generated by vLLM. To retrieve the torch compile cache, run vLLM and look for a log like below:
```
(RayWorkerWrapper pid=126782) INFO 10-15 11:57:04 [backends.py:608] Using cache directory: /home/ray/.cache/vllm/torch_compile_cache/131ee5c6d9/rank_1_0/backbone for vLLM's torch.compile
```

In this example the cache folder is located at `/home/ray/.cache/vllm/torch_compile_cache/131ee5c6d9`. Upload this directory to your S3 bucket. The cache folder can now be retrieved at startup. We provide a custom utility to download the compile cache from cloud storage. Specify the `CloudDownloader` callback in `LLMConfig` and supply the relevant arguments. Make sure to set the `cache_dir` in compilation_config correctly. 

```python
llm_config = LLMConfig(
    ...
    callback_config={
        "callback_class": "ray.llm._internal.common.callbacks.cloud_downloader.CloudDownloader",
        "callback_kwargs": {"paths": [("s3://samplebucket/llama-3-8b-cache", "/home/ray/.cache/vllm/torch_compile_cache/llama-3-8b-cache")]},
    },
    engine_kwargs={
        "tensor_parallel_size": 1,
        "compilation_config": {
            "cache_dir": "/home/ray/.cache/vllm/torch_compile_cache/llama-3-8b-cache",
        }
    },
    ...
)
```
Other options for retrieving the compile cache (distributed filesystem, block storage) can be used, as long as the path to the cache is set in `compilation_config`. 

### Custom Initialization Behaviors

We provide the ability to create custom node initialization behaviors with the API defined by [`CallbackBase`](https://github.com/ray-project/ray/blob/master/python/ray/llm/_internal/common/callbacks/base.py). Callback functions defined in the class are invoked at certain parts of the initialization process. An example is the above mentioned [`CloudDownloader`](https://github.com/ray-project/ray/blob/master/python/ray/llm/_internal/common/callbacks/cloud_downloader.py) which overrides the `on_before_download_model_files_distributed` function to distribute download tasks across nodes. To enable your custom callback, specify the classname inside `LLMConfig`. 

```python
from user_custom_classes import CustomCallback
config = LLMConfig(
    ...
    callback_config={
        "callback_class": CustomCallback, 
        # or use string "user_custom_classes.CustomCallback"
        "callback_kwargs": {"kwargs_test_key": "kwargs_test_value"},
    },
    ...
)
```

> **Note:** Callbacks are a new feature. We may change the callback API and incorporate user feedback as we continue to develop this functionality.


## Best practices

### Model source selection

- **Use Hugging Face** for publicly available models and quick prototyping
- **Use remote storage** for private models, custom fine-tunes, or when co-located with compute
- **Enable fast downloads** when downloading large models from Hugging Face

### Security

- **Never commit tokens** to version control. Use environment variables or secrets management.
- **Use IAM roles** instead of access keys for production deployments on AWS.
- **Scope permissions** to read-only access for model loading.

### Performance

- **Co-locate storage and compute** in the same cloud region to reduce latency and egress costs.
- **Use fast download** (`HF_HUB_ENABLE_HF_TRANSFER`) for models larger than 10GB.
- **Cache models** locally if you're repeatedly deploying the same model.
- **See benchmarks** [here](../benchmarks.md) for detailed information about optimizations

## Troubleshooting

### Slow downloads from Hugging Face

- Install `hf_transfer`: `pip install hf_transfer`
- Set `HF_HUB_ENABLE_HF_TRANSFER=1` in `runtime_env`
- Consider moving the model to S3/GCS in your cloud region and using RunAI streamer, and use sharding for large models

### S3/GCS access errors

- Verify bucket URI format (for example, `s3://bucket/path` or `gs://bucket/path`)
- Check AWS/GCP credentials and regions are configured correctly
- Ensure your IAM role or service account has `s3:GetObject` or `storage.objects.get` permissions
- Verify the bucket exists and is accessible from your deployment region

### Model files not found

- Verify the model structure matches Hugging Face format (must include `config.json`, tokenizer files, and model weights)
- Check that all required files are present in the bucket

## See also

- {doc}`Quickstart <../quick-start>` - Basic LLM deployment examples

