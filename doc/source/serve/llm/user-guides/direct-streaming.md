(direct-streaming-guide)=
# Direct streaming

Lower streaming latency by removing the ingress proxy hop and routing requests directly to model replicas.

:::{warning}
This feature is in alpha and may change before becoming stable. It depends on the HAProxy ingress and currently supports a single model per application.

The ingress request router is an internal component. Its router deployment, the `/internal/route` endpoint, and the replica-selection plumbing are private implementation details that may change without notice. Configure direct streaming only through the environment variables and the public `request_router_config` described in this guide; don't import or call those internals directly.
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
                  ┌─ (1) which replica? ──→ ingress request router (internal)
                  │                             │ applies request_router_config
Client → HAProxy ─┤                             ↓
                  │                          host:port of an LLMServer replica
                  └─ (2) forward request ──→ LLMServer replica → engine
```

The `LLMServer` replica builds its ASGI app from the engine's native OpenAI-compatible FastAPI app (for example vLLM's API server) after the engine starts, so streaming responses are served directly from the engine frontend.

### Ingress request router

Ray Serve adds an internal router deployment that answers HAProxy's routing calls. For each request, HAProxy asks the router which replica to use over an internal endpoint, and the router returns that replica's backend host and port.

Replica selection reuses the `LLMServer` deployment's configured request router, so the same routing policies you would use for any deployment apply here. When you don't configure one, direct streaming defaults to `RoundRobinRouter`. You control it through the public `request_router_config` (described in {ref}`direct-streaming-customize`); the router deployment and its endpoint are internal and may change.

The router picks a replica without reserving a capacity slot: the real request travels out-of-band through HAProxy, so Serve's capacity semaphore isn't load-bearing on this path, and skipping the reservation avoids an extra actor RPC per request.

## Enable direct streaming

Direct streaming runs on top of the HAProxy ingress. Set both environment variables before starting Serve:

```bash
export RAY_SERVE_ENABLE_HA_PROXY=1
export RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING=1
```

Then build and deploy a single-model application as usual:

```{literalinclude} ../../../llm/doc_code/serve/direct_streaming/direct_streaming_example.py
:start-after: __direct_streaming_example_start__
:end-before: __direct_streaming_example_end__
:language: python
```

When direct streaming is active, Serve logs a confirmation at build time that the LLM server is acting as the ingress with an ingress request router attached.

:::{tip}
The HAProxy ingress sets `TCP_NODELAY` by default (`RAY_SERVE_HAPROXY_TCP_NODELAY=1`) so the first streamed chunk isn't held back by Nagle's algorithm. Keep it enabled for streaming workloads.
:::

(direct-streaming-customize)=
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

### Body-aware routers

Some policies score replicas using the request body, for example {ref}`prefix-aware routing <prefix-aware-routing-guide>`, which keys on the prompt or messages. By default HAProxy doesn't forward the request body to the router, because buffering and re-emitting large bodies adds time to first token. Body-independent policies are unaffected: round-robin and power of two ignore the body, and session-aware policies key on the header.

If your policy needs the body, enable forwarding:

```bash
export RAY_SERVE_INGRESS_REQUEST_ROUTER_FORWARD_BODY=1
```

With forwarding on, HAProxy buffers the request body and includes it in the routing call. To bound the memory this costs, it buffers only up to `RAY_SERVE_HAPROXY_INGRESS_REQUEST_ROUTER_BUFSIZE` bytes (default 256 KiB; memory scales roughly as `2 * bufsize * maxconn`). When a request body is larger than that cap, HAProxy forwards only the leading bytes it captured and flags the routing call as carrying a truncated body, so the policy knows it's scoring against a prefix rather than the full payload. Truncation is fine for prefix-based policies, which only need the head of the prompt. Raise the buffer size if your policy needs to see more of large requests, keeping the memory tradeoff in mind.

### Session affinity

To pin all turns of a conversation to the same replica, send a session-id header with each request. HAProxy forwards the header to the ingress request router, which passes the session id to the configured policy. Session-aware policies such as `ConsistentHashRouter` then route every request with the same session id to one replica.

The header name defaults to `x-session-id` and is configurable with `RAY_SERVE_SESSION_ID_HEADER_KEY`. Matching is case-insensitive and tolerant of `-` / `_` rewrites that some proxies introduce.

(direct-streaming-limitations)=
## Limitations

- **HAProxy required.** Direct streaming relies on HAProxy to forward traffic to replicas, so it only takes effect with `RAY_SERVE_ENABLE_HA_PROXY=1`.
- **Single model per application.** `build_openai_app` raises if you pass more than one `LLMConfig` while direct streaming is enabled. Multi-model direct streaming isn't supported yet.
- **No separate ingress configuration.** Because the `LLMServer` deployment is the ingress, `ingress_deployment_config` and `ingress_cls_config` aren't supported. Configure the server through each `LLMConfig.deployment_config` instead.
- **Body-aware routing is opt-in.** HAProxy doesn't forward the request body to the router by default. Set `RAY_SERVE_INGRESS_REQUEST_ROUTER_FORWARD_BODY=1` for policies that need it, such as prefix-aware routing.
- **Single router replica.** The ingress request router runs with `num_replicas=1`.

## See also

- {ref}`routing-policies-guide` - request routing concepts and available policies
- {ref}`prefix-aware-routing-guide` - cache-locality routing policy
- {ref}`custom-request-router-guide` - implement a custom request router
