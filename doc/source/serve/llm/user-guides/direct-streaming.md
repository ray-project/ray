(serve-llm-direct-streaming-guide)=
# Direct streaming

Serve the inference engine's native OpenAI-compatible app directly from the `LLMServer` replica, removing the standalone ingress deployment from the request path.

:::{warning}
Direct streaming is an experimental, opt-in feature. It's controlled by the `RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING` environment variable and currently supports a single LLM config per application. The configuration surface may change in future releases.
:::

## How direct streaming works

In the {ref}`standard architecture <serve-llm-architecture-overview>`, a separate `OpenAiIngress` deployment terminates HTTP, applies routing logic, and forwards each request to an `LLMServer` replica over a deployment-handle RPC. Streamed tokens travel back along the same path, which means every chunk crosses the ingress-to-server boundary.

With direct streaming, the `LLMServer` deployment is itself the ingress. The engine's native ASGI app (for example, vLLM's FastAPI app) is built on the replica after the engine starts and serves the OpenAI endpoints directly. Replica selection still happens through an ingress request router attached to the deployment, so traffic spreads across replicas without the separate ingress hop or the extra per-chunk serialization.

This removes the intermediate deployment for streaming-heavy workloads at the cost of the flexibility the standalone ingress provides (see [Limitations](#limitations)).

## Enable direct streaming

Set `RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING=1` in the environment where you build and deploy the application. Ray Serve LLM reads the flag when the app graph is constructed, so export it before launching Serve rather than setting it after importing `ray.serve.llm`. Add it to the application's `runtime_env` as well so replicas inherit the same value.

::::{tab-set}
:::{tab-item} Python
```python
# Export before running this script so the builder sees the flag:
#   export RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING=1
from ray.serve.llm import LLMConfig, build_openai_app
import ray.serve as serve

llm_config = LLMConfig(
    model_loading_config={
        "model_id": "qwen-0.5b",
        "model_source": "Qwen/Qwen2-0.5B-Instruct",
    },
    engine_kwargs={"tensor_parallel_size": 1},
    runtime_env={
        "env_vars": {
            # Propagate the flag to replicas.
            "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING": "1",
        }
    },
)

app = build_openai_app({"llm_configs": [llm_config]})
serve.run(app)
```
:::

:::{tab-item} YAML
```yaml
applications:
  - name: llm-direct-streaming
    route_prefix: /
    import_path: ray.serve.llm:build_openai_app
    runtime_env:
      env_vars:
        RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING: "1"
    args:
      llm_configs:
        - model_loading_config:
            model_id: qwen-0.5b
            model_source: Qwen/Qwen2-0.5B-Instruct
          engine_kwargs:
            tensor_parallel_size: 1
```

Export the flag before deploying so the controller picks it up when it builds the application:

```bash
export RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING=1
serve run config.yaml
```
:::
::::

The deployed application is OpenAI-compatible and exposes the engine's native routes, including `/v1/chat/completions`, `/v1/completions`, and `/v1/models`. Ray Serve LLM also adds `GET /v1/models/{id}` so clients can call `client.models.retrieve(...)` as they would against the standalone ingress.

## Supported serving patterns

Direct streaming works with the single-model builders for the OpenAI, data parallel attention, and prefill/decode patterns:

- **OpenAI** (`build_openai_app`): the `LLMServer` deployment serves the engine app directly.
- **Data parallel attention** (`build_dp_openai_app`): the `DPServer` deployment serves the engine app directly. See {doc}`data-parallel-attention`.
- **Prefill/decode disaggregation** (`build_pd_openai_app`): the decode server serves the engine app directly, and `/v1/chat/completions` and `/v1/completions` route through prefill/decode orchestration (remote prefill, then local decode). Other routes stay engine-native. See {doc}`prefill-decode`.

## Default routing

Because the request router moves onto the LLM deployment, direct streaming defaults to `RoundRobinRouter` when you haven't set a router. If an `LLMConfig` already specifies a `request_router_config`, Ray Serve LLM leaves it untouched. See {doc}`prefix-aware-routing` for configuring a custom router.

## Limitations

Direct streaming uses the LLM server class as the ingress, which constrains the configuration:

- **Single LLM config only.** An application can serve exactly one model. Multi-model serving requires the standalone ingress, which composes multiple `LLMServer` deployments behind one app.
- **No `ingress_deployment_config`.** Configure the server through each `LLMConfig.deployment_config` instead.
- **No `ingress_cls_config`.** You can't swap in a custom ingress class or pass ingress extra kwargs, because there's no separate ingress deployment to configure.

If you need any of these, deploy without direct streaming and use the standalone `OpenAiIngress`.

## See also

- {ref}`Architecture overview <serve-llm-architecture-overview>` - The standard ingress and `LLMServer` architecture
- {doc}`prefix-aware-routing` - Configure a custom request router
- {doc}`prefill-decode` - Prefill/decode disaggregation
- {doc}`data-parallel-attention` - Data parallel attention
