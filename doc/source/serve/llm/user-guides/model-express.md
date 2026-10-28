---
myst:
  html_meta:
    description: "Scale out Ray Serve LLM faster by loading weights and compile caches from a running replica over GPU-to-GPU transfers with ModelExpress."
---

(model-express-guide)=
# Fast scale-out with ModelExpress

Load a new replica's weights and compile caches from a replica that is already serving, instead of from storage.

:::{note}
ModelExpress support is alpha and may change.
:::

[ModelExpress](https://github.com/ai-dynamo/modelexpress) (MX) moves model weights GPU to GPU over [NIXL](https://github.com/ai-dynamo/nixl). With `load_format="modelexpress"`, a scaled-out replica copies the weights of a running replica and installs its `torch.compile`, Triton, FlashInfer, and DeepGEMM caches, so it skips both the download and most of the warm-up.

## How it works

```{figure} ../images/model_express.svg
---
width: 100%
name: model-express-architecture
---
A new replica finds a serving replica through the MX server and copies its weights and compile caches GPU to GPU.
```

- **MX server:** a Serve app (named `mx` by default) that runs one replica on the head node. It's a registry of serving replicas; no weights pass through it.
- **First replica:** loads from storage as usual. Once its engine is up, it registers with the MX server as a source and publishes its compile caches.
- **Later replicas:** look up a source in the MX server, then pull weights and caches from it directly. If no source is available, they load from storage.

## Deploy

Deploy the `mx` app next to your LLM app and set `load_format` to `modelexpress`:

```yaml
applications:
  - name: mx
    route_prefix: null
    import_path: ray.serve.llm.mx:build_mx_app

  - name: llm
    route_prefix: /
    import_path: ray.serve.llm:build_openai_app
    runtime_env:
      env_vars:
        MX_NIXL_BACKEND: LIBFABRIC  # AWS EFA across nodes; UCX within a node
    args:
      llm_configs:
        - model_loading_config:
            model_id: Qwen/Qwen3-235B-A22B-Instruct-2507-FP8
          deployment_config:
            autoscaling_config:
              min_replicas: 1
              max_replicas: 4
          engine_kwargs:
            load_format: modelexpress
            tensor_parallel_size: 8
            enable_expert_parallel: true
```

Or in Python:

```python
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app
from ray.serve.llm.mx import build_mx_app

serve.run(build_mx_app(), name="mx", route_prefix=None)

llm_config = LLMConfig(
    model_loading_config=dict(model_id="Qwen/Qwen3-0.6B"),
    deployment_config=dict(autoscaling_config=dict(min_replicas=1, max_replicas=4)),
    engine_kwargs=dict(load_format="modelexpress"),
)
serve.run(build_openai_app({"llm_configs": [llm_config]}), name="llm", route_prefix="/")
```

## Requirements

- **Image:** the `ray-llm` image ships the MX server binary and Python client. For a custom image, install both and set `RAY_MX_SERVER_BINARY_PATH`.
- **Network:** across nodes, NIXL needs RDMA. On AWS, set `MX_NIXL_BACKEND=LIBFABRIC` and expose the EFA devices to the Ray containers (for example, host networking on Kubernetes). Within a node, `UCX` uses CUDA IPC.

## Configuration

`build_mx_app` takes `{"metadata_backend": "memory"}` (default) or `"redis"`. Use `redis`, with `REDIS_URL` in the app's `runtime_env`, to keep the registry when the MX server restarts.

| Environment variable | Default | Purpose |
|---|---|---|
| `RAY_MX_APP_NAME` | `mx` | Name of the MX server app that LLM replicas look up. |
| `RAY_MX_SERVER_PORT` | `8931` | MX server gRPC port. |
| `RAY_MX_SERVER_BINARY_PATH` | `/usr/local/bin/modelexpress-server` | MX server binary. |
| `RAY_MX_METADATA_PORT_BASE` | `15555` | Base NIXL metadata port; each worker adds its GPU index. |
| `RAY_MX_WORKER_GRPC_PORT_BASE` | `16555` | Base worker gRPC port; each worker adds its GPU index. |

Variables that start with `MX_` configure the MX client itself; Ray Serve LLM forwards them to the vLLM workers.

## Example results

Qwen3-235B-A22B-Instruct-2507-FP8, TP=8 with expert parallelism, one replica per 8xH100 node with EFA:

| | First replica (from Hugging Face) | Second replica (from the first) |
|---|---|---|
| Weights | 336 s | 32 s (1.1 s transfer at ~218 Gbps per GPU) |
| `torch.compile` | 83 s | 1 s |
| Replica start to ready | ~15.5 min | ~4 min |

## Limitations

- The first replica still loads from storage and compiles. Keep `min_replicas` at 1 or more to keep a source available.
- After a source replica dies, the MX server keeps it registered for about 90 seconds. A replica that tries it in that window fails to connect, then falls back to another source or to storage.
- Streaming weights from object storage on a cold start (`MX_MODEL_URI`) needs `runai-model-streamer`, which the `ray-llm` image doesn't include.

## See also

- {doc}`Deployment initialization <deployment-initialization>`: other ways to speed up model loading.
- [ModelExpress](https://github.com/ai-dynamo/modelexpress)
