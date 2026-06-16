(serve-llm-configuration)=

# Configuration reference

{class}`LLMConfig <ray.serve.llm.LLMConfig>` is the central configuration object for a Ray Serve LLM deployment. It describes which model to serve, which engine and hardware to run it on, how to scale it, and how to expose it. You pass one `LLMConfig` per model to a builder such as {func}`build_openai_app <ray.serve.llm.build_openai_app>`, or you write the equivalent YAML.

This page documents every field. For the design behind these abstractions, see {doc}`Core components <../architecture/core>`.

## Minimal configuration

Only `model_loading_config` is required. Every other field has a default. The example also sets `accelerator_type` so replicas schedule onto the right GPU:

```python
from ray.serve.llm import LLMConfig

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="qwen-0.5b",
        model_source="Qwen/Qwen2.5-0.5B-Instruct",
    ),
    accelerator_type="A10G",
)
```

## Top-level fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `model_loading_config` | dict or {class}`~ray.serve.llm.ModelLoadingConfig` | required | Which model to serve and where to load it from. See [Model loading](#model-loading). |
| `engine_kwargs` | dict | `{}` | Arguments forwarded to the inference engine (vLLM). See [Engine arguments](#engine-arguments). |
| `accelerator_type` | str | `None` | The accelerator the model runs on, for example `"A10G"`. See [Accelerators](#accelerators-and-placement). |
| `accelerator_config` | dict | `None` | Hardware-specific config (GPU, TPU, CPU). Inferred from `accelerator_type` or `placement_group_config` when omitted. |
| `placement_group_config` | dict | `None` | Resource bundles and placement strategy for engine workers. See [Accelerators and placement](#accelerators-and-placement). |
| `deployment_config` | dict | `{}` | Ray Serve deployment options such as autoscaling and replicas. See [Deployment options](#deployment-options). |
| `lora_config` | dict or {class}`~ray.serve.llm.LoraConfig` | `None` | LoRA adapter settings. See [LoRA](#lora). |
| `runtime_env` | dict | `None` | Ray runtime environment applied to the replica and engine workers (for example, `env_vars`). |
| `llm_engine` | str | `"vLLM"` | The inference engine. `"vLLM"` is the supported value. |
| `server_cls` | str or class | `None` | Custom server class, for example `SGLangServer`. Accepts an import path string or a class. See {doc}`SGLang integration <sglang>`. |
| `experimental_configs` | dict | `{}` | Experimental knobs. See [Experimental configs](#experimental-configs). |
| `log_engine_metrics` | bool | `True` | Export engine metrics through the Ray Prometheus port. See {doc}`Observability <observability>`. |
| `callback_config` | `CallbackConfig` | default | Initialization callbacks. See {doc}`Deployment initialization <deployment-initialization>`. |

## Model loading

`model_loading_config` is validated against {class}`~ray.serve.llm.ModelLoadingConfig`:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `model_id` | str | required | The ID end users pass as `model` in API requests. |
| `model_source` | str or `CloudMirrorConfig` | `None` | Where to load weights from. When unset, defaults to `model_id` as a Hugging Face ID. |
| `tokenizer_source` | str | `None` | Where to load the tokenizer from. When unset, the tokenizer is loaded from the model source. Hugging Face IDs only. |

`model_source` accepts several forms:

```python
# Hugging Face Hub ID
model_loading_config=dict(model_id="qwen", model_source="Qwen/Qwen2.5-0.5B-Instruct")

# Local path
model_loading_config=dict(model_id="qwen", model_source="/mnt/models/qwen")

# Cloud storage mirror (S3, GCS, or Azure)
model_loading_config=dict(
    model_id="qwen",
    model_source=dict(bucket_uri="s3://my-bucket/path/to/qwen"),
)
```

A cloud mirror is validated against {class}`~ray.serve.llm.CloudMirrorConfig` and supports `s3://`, `gs://`, `abfss://`, and `azure://` URIs. Mirroring weights from cloud storage avoids per-replica Hugging Face downloads. See {doc}`Deployment initialization <deployment-initialization>`.

## Engine arguments

`engine_kwargs` are passed straight through to the engine, so any vLLM engine argument is valid here. Commonly used ones:

| Argument | Description |
|----------|-------------|
| `tensor_parallel_size` | Number of GPUs to shard each model replica across. See {doc}`Cross-node parallelism <cross-node-parallelism>`. |
| `pipeline_parallel_size` | Number of pipeline stages, used to span nodes. |
| `max_model_len` | Maximum context length (prompt plus generated tokens). |
| `gpu_memory_utilization` | Fraction of GPU memory the engine may use for weights and KV cache. |
| `max_num_seqs` | Maximum number of sequences batched together per step. |
| `kv_transfer_config` | KV connector settings for prefill/decode and cache offloading. See {doc}`KV cache offloading <kv-cache-offloading>`. |

For the full list, see the [vLLM engine arguments](https://docs.vllm.ai/en/latest/serving/engine_args.html). `disable_log_stats=True` is rejected when `log_engine_metrics` is enabled, because engine metrics require log stats.

## Accelerators and placement

Set `accelerator_type` to the accelerator each replica should be scheduled on. Common GPU values include `A10G`, `A100`, `H100`, `H200`, `L4`, `L40S`, and `T4`. TPUs such as `TPU-V4` and `TPU-V5P` are also supported. `A10` is normalized to `A10G`. An unsupported value raises a validation error.

`accelerator_config` is inferred from `accelerator_type` (or from `placement_group_config` bundles) and rarely needs to be set explicitly.

Use `placement_group_config` to control how engine workers are packed onto nodes, for example for multi-node tensor or pipeline parallelism. Provide either `bundle_per_worker` (auto-replicated by `tensor_parallel_size * pipeline_parallel_size`) or an explicit `bundles` list, plus an optional `strategy`:

```python
# One GPU bundle per worker, spread across nodes
placement_group_config=dict(
    bundle_per_worker={"CPU": 1, "GPU": 1},
    strategy="SPREAD",
)

# Explicit bundle list
placement_group_config=dict(
    bundles=[{"CPU": 1, "GPU": 1}] * 4,
    strategy="STRICT_PACK",
)
```

`strategy` is one of `PACK`, `STRICT_PACK`, `SPREAD`, or `STRICT_SPREAD`. See {doc}`Cross-node parallelism <cross-node-parallelism>` for worked examples.

## Deployment options

`deployment_config` holds standard Ray Serve `@serve.deployment` options. Supported keys: `name`, `num_replicas`, `ray_actor_options`, `max_ongoing_requests`, `autoscaling_config`, `max_queued_requests`, `user_config`, `health_check_period_s`, `health_check_timeout_s`, `graceful_shutdown_wait_loop_s`, `graceful_shutdown_timeout_s`, `logging_config`, and `request_router_config`.

```python
deployment_config=dict(
    autoscaling_config=dict(
        min_replicas=1,
        max_replicas=4,
    ),
    max_ongoing_requests=64,
)
```

Set `num_replicas="auto"` to let Serve autoscale. Use `request_router_config` to plug in a custom router, for example for {doc}`prefix-aware routing <prefix-aware-routing>`. For the meaning of each option, see the [Ray Serve deployment configuration](https://docs.ray.io/en/latest/serve/configure-serve-deployment.html) docs.

## LoRA

When `lora_config` is set, the deployment serves LoRA adapters on top of the base model. It is validated against {class}`~ray.serve.llm.LoraConfig`:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `dynamic_lora_loading_path` | str | `None` | Cloud storage path (`s3://`, `gs://`, `abfss://`, or `azure://`) holding adapter weights. |
| `max_num_adapters_per_replica` | int | `16` | Maximum adapters loaded on each replica, evicted LRU. |
| `download_timeout_s` | float | `30` | Per-adapter download timeout in seconds. A value `<= 0` disables the timeout. |
| `max_download_tries` | int | `3` | Maximum adapter download retries. |

See {doc}`Multi-LoRA deployment <multi-lora>` for the full workflow.

## Experimental configs

`experimental_configs` is a dictionary of opt-in knobs that may change between releases:

| Key | Description |
|-----|-------------|
| `stream_batching_interval_ms` | How long to batch streaming responses before flushing them. Defaults to `50`. |
| `num_ingress_replicas` | Number of ingress (router) replicas. |

## YAML equivalent

A config-file (YAML) deployment. `LLMConfig` fields map one to one onto YAML keys:

```yaml
applications:
- args:
    llm_configs:
    - model_loading_config:
        model_id: qwen-0.5b
        model_source: Qwen/Qwen2.5-0.5B-Instruct
      accelerator_type: A10G
      engine_kwargs:
        tensor_parallel_size: 2
        max_model_len: 8192
      deployment_config:
        autoscaling_config:
          min_replicas: 1
          max_replicas: 4
  import_path: ray.serve.llm:build_openai_app
  name: llm_app
  route_prefix: "/"
```

Deploy it with `serve run config.yaml`.

## See also

- {doc}`Quickstart <../quick-start>`: deploy and query a model.
- {class}`LLMConfig API reference <ray.serve.llm.LLMConfig>`: the generated API docs.
