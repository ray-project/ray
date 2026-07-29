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

Load models from S3, GCS, or Azure storage instead of Hugging Face. This is useful for:

- Private models not hosted on Hugging Face
- Faster loading from cloud storage in the same region
- Custom model formats or fine-tuned models

Select your cloud provider for backend-specific configuration:

`````{tab-set}

````{tab-item} S3

**Bucket structure**

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

**Configure with YAML**

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

**Configure with the Python API**

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

**Credentials**

For private S3 buckets, configure AWS credentials.

Option 1: Environment variables

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

Option 2: IAM roles (recommended for production)

Use EC2 instance profiles or EKS service accounts with appropriate S3 read permissions.

````

````{tab-item} GCS

**Configure with YAML**

For Google Cloud Storage, use the `gs://` protocol:

```yaml
model_loading_config:
  model_id: my_model
  model_source:
    bucket_uri: gs://my-gcs-bucket/path/to/model
```

````

````{tab-item} Azure

For Azure Blob Storage or Azure Data Lake Storage (ADLS) Gen2, use the `azure://`
or `abfss://` protocol. The URI must embed the container and storage account as
`container@account.<domain>`:

- `abfss://<container>@<account>.dfs.core.windows.net/path/to/model` (ADLS Gen2)
- `azure://<container>@<account>.blob.core.windows.net/path/to/model` (Blob Storage)

**Configure with YAML**

```yaml
model_loading_config:
  model_id: my_model
  model_source:
    bucket_uri: abfss://my-container@myaccount.dfs.core.windows.net/path/to/model
```

**Configure with the Python API**

Azure loading requires the `adlfs` and `azure-identity` packages on every node
that loads the model. Ship them through `runtime_env` so Ray installs them on
each node's download task, no image rebuild required:

```python
llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="my_model",
        model_source=dict(
            bucket_uri="abfss://my-container@myaccount.dfs.core.windows.net/path/to/model"
        ),
    ),
    runtime_env=dict(pip=["adlfs", "azure-identity"]),
)
```

**Credentials**

Azure authentication uses [`DefaultAzureCredential`](https://learn.microsoft.com/python/api/azure-identity/azure.identity.defaultazurecredential),
which resolves credentials from the standard Azure credential chain. It tries
several sources in order, including environment variables, a workload identity, a
managed identity, and the Azure CLI login.

For production deployments on AKS, use a [Microsoft Entra Workload
ID](https://learn.microsoft.com/azure/aks/workload-identity-overview), or a
managed identity, with the **Storage Blob Data Reader** role on the container.
`DefaultAzureCredential` resolves either one automatically, so the deployment
only needs the Azure packages. Uncomment the environment variables to fall back
to a service principal when a workload or managed identity isn't available:

```python
llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="my_model",
        model_source=dict(
            bucket_uri="abfss://my-container@myaccount.dfs.core.windows.net/model"
        )
    ),
    runtime_env=dict(
        pip=["adlfs", "azure-identity"],
        # A workload or managed identity resolves automatically and needs nothing here.
        # Uncomment to fall back to a service principal instead:
        # env_vars={
        #     "AZURE_CLIENT_ID": os.environ["AZURE_CLIENT_ID"],
        #     "AZURE_TENANT_ID": os.environ["AZURE_TENANT_ID"],
        #     "AZURE_CLIENT_SECRET": os.environ["AZURE_CLIENT_SECRET"],
        # },
    ),
)
```

````

`````


## RunAI Streamer

RunAI Streamer is a vLLM extension that streams model weights directly from remote storage into GPU memory, reducing model load latency.

:::{note}
These snippets are examples. Check the
[RunAI Streamer docs](https://docs.vllm.ai/en/stable/models/extensions/runai_model_streamer.html)
for S3, Azure, and GCS compatibility with your vLLM version.
:::

### S3 and RunAI Streamer

Set `model_source` to an `s3://` URI and `load_format` to `runai_streamer`:

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

### RunAI Streamer from a local path

When `load_format` is `runai_streamer`, Ray Serve LLM doesn't download the model. It passes `model_source` to the streamer, which reads it directly. The streamer supports a local path on each node in addition to remote object stores, and the set of supported remote schemes depends on your `runai-model-streamer` and vLLM versions. Use a local path when the weights are already staged on a volume mounted on every node or copied to local disk, such as weights pulled from another source before serving. Point `model_source` at the path, set `load_format` to `runai_streamer`, and tune the number of concurrent read streams with the `RUNAI_STREAMER_CONCURRENCY` environment variable:

```python
from ray.serve.llm import LLMConfig

llm_config = LLMConfig(
    model_loading_config={
        "model_id": "my-model",
        "model_source": "/path/to/model",
    },
    engine_kwargs={
        "tensor_parallel_size": 16,
        "load_format": "runai_streamer",
    },
    runtime_env={"env_vars": {"RUNAI_STREAMER_CONCURRENCY": "16"}},
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

### Azure Blob streaming with RunAI Streamer

RunAI Streamer reads Azure Blob Storage natively through the `az://` scheme, streaming weights into GPU memory without staging them on disk first. This requires versions of `runai-model-streamer` and vLLM that support `az://`, so confirm the versions bundled in your image.

Set `model_source` to an `az://<container>/<model>` URI and `load_format` to `runai_streamer`. The `AZURE_STORAGE_ACCOUNT_NAME` environment variable tells the streamer which storage account to read from. On AKS, bind the pod to a workload identity that holds the **Storage Blob Data Reader** role so the streamer authenticates with a Microsoft Entra ID token instead of a key.

```python
from ray.serve.llm import LLMConfig

llm_config = LLMConfig(
    model_loading_config={
        "model_id": "phi-4",
        "model_source": "az://models/phi-4",
    },
    engine_kwargs={
        "tensor_parallel_size": 1,
        "load_format": "runai_streamer",
    },
    runtime_env={"env_vars": {"AZURE_STORAGE_ACCOUNT_NAME": "mystorageacct"}},
)
```

Unlike the `abfss://` and `azure://` schemes, which download the model to disk before loading, the `az://` scheme streams directly from Blob into GPU memory.

For an end-to-end AKS walkthrough that provisions the cluster, workload identity, and Blob storage, and benchmarks streaming against download-then-load, see [Stream models from Azure Blob Storage into vLLM with the RunAI Model Streamer](https://blog.aks.azure.com/2026/07/13/runai-streamer-vllm).

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
- Consider moving the model to S3, GCS, or Azure storage in your cloud region and using RunAI streamer, and use sharding for large models

### Cloud storage access errors

- Verify bucket or container URI format (for example, `s3://bucket/path`, `gs://bucket/path`, or `abfss://container@account.dfs.core.windows.net/path`)
- Check AWS/GCP/Azure credentials and regions are configured correctly
- Ensure your IAM role, service account, or Azure identity has read access (`s3:GetObject`, `storage.objects.get`, or the **Storage Blob Data Reader** role)
- For Azure, confirm the `adlfs` and `azure-identity` Python packages are installed on every node
- Verify the bucket or container exists and is accessible from your deployment region

### Model files not found

- Verify the model structure matches Hugging Face format (must include `config.json`, tokenizer files, and model weights)
- Check that all required files are present in the bucket

## See also

- {doc}`Quickstart <../quick-start>` - Basic LLM deployment examples

