(model-loading-guide)=
# Model loading

Configure model loading from Hugging Face, remote storage, or gated repositories.

Ray Serve LLM supports loading models from multiple sources:

- **Hugging Face Hub**: Load models directly from Hugging Face (default)
- **Remote storage**: Load from S3 or GCS buckets
- **Gated models**: Access private or gated Hugging Face models with authentication

You configure model loading through the `model_loading_config` parameter in `LLMConfig`.

## Load from Hugging Face Hub

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

You can also use third-party integrations for streaming models directly to GPU, such as Run:ai Model Streamer.

## Load gated models

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


## Load from remote storage

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

## Troubleshooting

### Slow downloads from Hugging Face

- Install `hf_transfer`: `pip install hf_transfer`
- Set `HF_HUB_ENABLE_HF_TRANSFER=1` in `runtime_env`
- Consider moving the model to S3/GCS in your cloud region

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

