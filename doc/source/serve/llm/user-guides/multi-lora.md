# Multi-LoRA deployment

Deploy multiple fine-tuned LoRA adapters efficiently with Ray Serve LLM.

## Understand multi-LoRA deployment

Multi-LoRA lets your model switch between different fine-tuned adapters at runtime without reloading the base model.

Use multi-LoRA when your application needs to support multiple domains, users, or tasks using a single shared model backend. Following are the main reasons you might want to add adapters to your workflow:

- **Parameter efficiency**: LoRA adapters are small, typically less than 1% of the base model's size. This makes them cheap to store, quick to load, and easy to swap in and out during inference, which is especially useful when memory is tight.
- **Runtime adaptation**: With multi-LoRA, you can switch between different adapters at inference time without reloading the base model. This allows for dynamic behavior depending on user, task, domain, or context, all from a single deployment.
- **Simpler MLOps**: Multi-LoRA cuts down on infrastructure complexity and cost by centralizing inference around one model.

### How request routing works

When a request for a given LoRA adapter arrives, Ray Serve:

1. Checks if any replica has already loaded that adapter
2. Finds a replica with the adapter but isn't overloaded and routes the request to it
3. If all replicas with the adapter are overloaded, routes the request to a less busy replica, which loads the adapter
4. If no replica has the adapter loaded, routes the request to a replica according to the default request router logic (for example Power of 2) and loads it there

Ray Serve LLM then caches the adapter for subsequent requests. Ray Serve LLM controls the cache of LoRA adapters on each replica through a Least Recently Used (LRU) mechanism with a max size, which you control with the `max_num_adapters_per_replica` variable.


## Configure Ray Serve LLM with multi-LoRA

To enable multi-LoRA on your deployment, update your Ray Serve LLM configuration with these additional settings.

### LoRA configuration

Set `dynamic_lora_loading_path` to your AWS or GCS storage path:

```python
lora_config=dict(
    dynamic_lora_loading_path="s3://my_dynamic_lora_path",
    max_num_adapters_per_replica=16,  # Optional: limit adapters per replica
)
```

- `dynamic_lora_loading_path`: Path to the directory containing LoRA checkpoint subdirectories.
- `max_num_adapters_per_replica`: Maximum number of LoRA adapters cached per replica. Must match `max_loras`.


### Engine arguments

Forward these parameters to your vLLM engine:

```python
engine_kwargs=dict(
    enable_lora=True,
    max_lora_rank=32,  # Set to the highest LoRA rank you plan to use
    max_loras=16,      # Must match max_num_adapters_per_replica
)
```

- `enable_lora`: Enable LoRA support in the vLLM engine.
- `max_lora_rank`: Maximum LoRA rank supported. Set to the highest rank you plan to use.
- `max_loras`: Maximum number of LoRAs per batch. Must match `max_num_adapters_per_replica`.

### Example

The following example shows a complete multi-LoRA configuration:

```python
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app

# Configure the model with LoRA
llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="qwen-0.5b",
        model_source="Qwen/Qwen2.5-0.5B-Instruct",
    ),
    lora_config=dict(
        # Assume this is where LoRA weights are stored on S3.
        # For example
        # s3://my_dynamic_lora_path/lora_model_1_ckpt
        # s3://my_dynamic_lora_path/lora_model_2_ckpt
        # are two of the LoRA checkpoints
        dynamic_lora_loading_path="s3://my_dynamic_lora_path",
        max_num_adapters_per_replica=16, # Need to set this to the same value as `max_loras`.
    ),
    engine_kwargs=dict(
        enable_lora=True,
        max_loras=16, # Need to set this to the same value as `max_num_adapters_per_replica`.
    ),
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1,
            max_replicas=2,
        )
    ),
    accelerator_type="A10G",
)

# Build and deploy the model
app = build_openai_app({"llm_configs": [llm_config]})
serve.run(app, blocking=True)
```

## Send requests to multi-LoRA adapters

To query the base model, call your service as you normally would.

To use a specific LoRA adapter at inference time, include the adapter name in your request using the following format:

```
<base_model_id>:<adapter_name>
```

where
- `<base_model_id>` is the `model_id` that you define in the Ray Serve LLM configuration
- `<adapter_name>` is the adapter's folder name in your cloud storage

### Example queries

Query both the base model and different LoRA adapters:

```python
from openai import OpenAI

client = OpenAI(base_url="http://localhost:8000/v1", api_key="fake-key")

# Base model request (no adapter)
response = client.chat.completions.create(
    model="qwen-0.5b",  # No adapter
    messages=[{"role": "user", "content": "Hello!"}],
)

# Adapter 1
response = client.chat.completions.create(
    model="qwen-0.5b:adapter_name_1",  # Follow naming convention in your cloud storage
    messages=[{"role": "user", "content": "Hello!"}],
    stream=True,
)

for chunk in response:
    if chunk.choices[0].delta.content is not None:
        print(chunk.choices[0].delta.content, end="", flush=True)

# Adapter 2
response = client.chat.completions.create(
    model="qwen-0.5b:adapter_name_2",
    messages=[{"role": "user", "content": "Hello!"}],
)
```

## See also

- {doc}`Quickstart <../quick-start>`
- [vLLM LoRA documentation](https://docs.vllm.ai/en/stable/models/lora.html)

