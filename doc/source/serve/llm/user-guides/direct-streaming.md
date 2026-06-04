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

To confirm direct streaming is active, open the Serve dashboard and check that the ingress request router deployment (listed as `LLMRouter`) is running alongside your model deployment.

:::{tip}
The HAProxy ingress sets `TCP_NODELAY` by default (`RAY_SERVE_HAPROXY_TCP_NODELAY=1`) so the first streamed chunk isn't held back by Nagle's algorithm. Keep it enabled for streaming workloads.
:::

(direct-streaming-customize)=
## Customize replica selection

Direct streaming uses the deployment's `request_router_config`, so you select a routing policy the same way you would for any LLM deployment. Set it on the model's `deployment_config`:

```{literalinclude} ../../../llm/doc_code/serve/direct_streaming/direct_streaming_custom_router_example.py
:start-after: __direct_streaming_custom_router_example_start__
:end-before: __direct_streaming_custom_router_example_end__
:language: python
```

If you set `request_router_config`, direct streaming uses it as-is. Otherwise it falls back to `RoundRobinRouter`. For the available policies and how to write your own, see {ref}`routing-policies-guide` and {ref}`custom-request-router-guide`.

### Body-aware routers

Some policies score replicas using the request body, for example {ref}`prefix-aware routing <prefix-aware-routing-guide>`, which keys on the prompt or messages. By default HAProxy doesn't forward the request body to the router, because buffering and re-emitting large bodies adds time to first token. Body-independent policies are unaffected: round-robin and power of two ignore the body, and session-aware policies key on the header.

If your policy needs the body, enable forwarding:

```bash
export RAY_SERVE_INGRESS_REQUEST_ROUTER_FORWARD_BODY=1
```

With forwarding on, HAProxy has to receive and buffer the request body before it can route, and that wait is what adds to TTFT: the more of the body it waits for, the longer routing is delayed (and the more memory it holds). To bound that cost, HAProxy buffers only up to `RAY_SERVE_HAPROXY_INGRESS_REQUEST_ROUTER_BUFSIZE` bytes. When a request body is larger than that cap, HAProxy stops waiting, routes on the leading bytes it already has, and flags the routing call as carrying a truncated body, so the policy knows it's scoring against a prefix rather than the full payload.

Truncation affects only the copy sent to the router, not the request forwarded to the replica or the response to the client. The captured portion is always the head of the body, which is what prefix-based policies match on. To tune the cap against real traffic, watch the `serve_haproxy_ingress_router_truncations_total` metric (enable the ingress request router metrics with `RAY_SERVE_INGRESS_REQUEST_ROUTER_METRICS_ENABLED=1`): a high truncation rate means body-aware policies are routing on clipped prompts and may warrant a larger buffer. See [HAProxy ingress request router metrics](../../monitoring.md#haproxy-ingress-request-router-metrics) for the full set.

### Session affinity

To pin all turns of a conversation to the same replica, send a session-id header with each request. HAProxy forwards the header to the ingress request router, which passes the session id to the configured policy. Session-aware policies such as `ConsistentHashRouter` then route every request with the same session id to one replica.

The header name defaults to `x-session-id` and is configurable with `RAY_SERVE_SESSION_ID_HEADER_KEY`. Header matching ignores case and treats `-` and `_` as equivalent, so `X-Session-Id`, `x-session-id`, and `x_session_id` all match the same configured name. This matters because some reverse proxies (for example nginx and AWS API Gateway) rewrite `-` to `_` in header names; without this tolerance, such a rewrite would silently drop session affinity.

(direct-streaming-limitations)=
## Limitations

- **Single model per application.** `build_openai_app` raises if you pass more than one `LLMConfig` while direct streaming is enabled. To serve multiple models, deploy each as its own single-model direct streaming application on a distinct route prefix; clients target the per-model endpoint directly instead of selecting the model by the `model` field on one shared endpoint.
- **No LoRA- or multiplex-aware routing.** The ingress request router doesn't forward the requested model or adapter id to the routing policy, so requests aren't steered to replicas that already have a given LoRA adapter loaded (the default `RoundRobinRouter` is multiplex-unaware). A single base model with adapters still serves, but without adapter affinity. If you need adapter-affinity routing, use the default (non-direct) ingress, which routes multiplex-aware; see [Multi-LoRA deployment](multi-lora.md).

## See also

- {ref}`routing-policies-guide` - request routing concepts and available policies
- {ref}`prefix-aware-routing-guide` - cache-locality routing policy
- {ref}`custom-request-router-guide` - implement a custom request router
