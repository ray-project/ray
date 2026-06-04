(direct-streaming-guide)=
# Direct streaming

Lower streaming latency by removing the ingress proxy hop and routing requests directly to model replicas.

:::{warning}
This feature is in alpha and may change before becoming stable. It depends on the HAProxy ingress and currently supports a single model per application.
:::

By default, every request to a Ray Serve LLM application flows through a separate ingress deployment (`OpenAiIngress`) before reaching an `LLMServer` replica. The ingress replica proxies both the request and the streamed response, so each token in a streaming response crosses one extra deployment boundary.

**Direct streaming** removes that hop. When enabled, the `LLMServer` deployment itself becomes the HTTP ingress, and HAProxy forwards client traffic straight to the replica that serves it. An **ingress request router** decides which replica each request goes to.

## When to use direct streaming

Use direct streaming when:

- You serve a single model and want to minimize streaming latency.
- You already run the HAProxy ingress (`RAY_SERVE_ENABLE_HA_PROXY=1`).
- Per-hop overhead on long streaming responses matters for your workload.

Keep the default ingress when you serve multiple models behind one endpoint, or when you customize the ingress with `ingress_cls_config` or `ingress_deployment_config` (see {ref}`direct-streaming-limitations`).

## How it works

Without direct streaming, the request and every streamed token pass through the ingress deployment:

```
Client → HAProxy → OpenAiIngress replica → LLMServer replica → engine
```

With direct streaming, the `LLMServer` deployment is the ingress. HAProxy first asks the ingress request router which replica to use, then forwards the request directly to that replica's backend HTTP port:

```
                  ┌─ (1) /internal/route ─→ LLMRouter (ingress request router)
                  │                             │ choose_replica()
Client → HAProxy ─┤                             ↓
                  │                          host:port of an LLMServer replica
                  └─ (2) forward request ──→ LLMServer replica → engine
```

The `LLMServer` replica builds its ASGI app from the engine's native OpenAI-compatible FastAPI app (for example vLLM's API server) after the engine starts, so streaming responses are served directly from the engine frontend.

### Ingress request router

The ingress request router is a small deployment (`LLMRouter`) that answers HAProxy's routing calls. For each request, HAProxy posts to `/internal/route`, and the router returns the `host`, `port`, and `replica_id` of the replica that should handle it.

Replica selection is delegated to the `LLMServer` deployment's configured request router through `choose_replica()`, so the same routing policies used elsewhere in Ray Serve apply here. When you don't configure a router, direct streaming defaults to `RoundRobinRouter`.

The router picks a replica without reserving a capacity slot. The real request travels out-of-band through HAProxy, so Serve's capacity semaphore isn't load-bearing on this path, and skipping the reservation avoids an extra actor RPC per request.

## Enable direct streaming

Direct streaming runs on top of the HAProxy ingress. Set both environment variables before starting Serve:

```bash
export RAY_SERVE_ENABLE_HA_PROXY=1
export RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING=1
```

Then build and deploy a single-model application as usual:

```python
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app

llm_config = LLMConfig(
    model_loading_config={"model_id": "qwen-0.5b"},
    deployment_config={"autoscaling_config": {"min_replicas": 1, "max_replicas": 4}},
)

app = build_openai_app({"llm_configs": [llm_config]})
serve.run(app)
```

When direct streaming is active, the build logs confirm the wiring:

```
Direct streaming enabled: LLMServer=ingress, LLMRouter=ingress_request_router
```

## Customize replica selection

Direct streaming uses the deployment's `request_router_config`, so you select a routing policy the same way you would for any LLM deployment. Set it on the model's `deployment_config`:

```python
from ray.serve.config import RequestRouterConfig
from ray.serve.llm import LLMConfig

llm_config = LLMConfig(
    model_loading_config={"model_id": "qwen-0.5b"},
    deployment_config={
        "request_router_config": RequestRouterConfig(
            request_router_class="ray.serve.experimental.consistent_hash_router.ConsistentHashRouter",
        ),
    },
)
```

If you set `request_router_config`, direct streaming uses it as-is. Otherwise it falls back to `RoundRobinRouter`. For the available policies and how to write your own, see {ref}`routing-policies-guide` and {ref}`custom-request-router-guide`.

### Session affinity

To pin all turns of a conversation to the same replica, send a session-id header with each request. HAProxy forwards the header to the ingress request router, which applies it through `handle.options(session_id=...)` before calling `choose_replica()`. Session-aware policies such as `ConsistentHashRouter` then route every request with the same session id to one replica.

The header name defaults to `x-session-id` and is configurable with `RAY_SERVE_SESSION_ID_HEADER_KEY`. Matching is case-insensitive and tolerant of `-` / `_` rewrites that some proxies introduce.

(direct-streaming-limitations)=
## Limitations

- **HAProxy required.** Direct streaming relies on HAProxy to forward traffic to replicas, so it only takes effect with `RAY_SERVE_ENABLE_HA_PROXY=1`.
- **Single model per application.** `build_openai_app` raises if you pass more than one `LLMConfig` while direct streaming is enabled. Multi-model direct streaming isn't supported yet.
- **No separate ingress configuration.** Because the `LLMServer` deployment is the ingress, `ingress_deployment_config` and `ingress_cls_config` aren't supported. Configure the server through each `LLMConfig.deployment_config` instead.
- **Single router replica.** The ingress request router runs with `num_replicas=1`.

## See also

- {ref}`routing-policies-guide` - request routing concepts and available policies
- {ref}`prefix-aware-routing-guide` - cache-locality routing policy
- {ref}`custom-request-router-guide` - implement a custom request router
