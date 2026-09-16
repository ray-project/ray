---
myst:
  html_meta:
    description: "Lower streaming latency by routing requests straight to model replicas, bypassing the ingress proxy hop, with body-aware routers."
---

(direct-streaming-guide)=
# Direct streaming

Lower streaming latency by removing the ingress proxy hop and routing requests directly to model replicas.

:::{note}
Direct streaming is experimental and may change before it becomes stable. It depends on the HAProxy ingress. Configure it through the environment variables and `request_router_config` described in this guide rather than the internal ingress and router deployments and their endpoints.
:::

By default, every request to a Ray Serve LLM application flows through a separate ingress deployment (`OpenAiIngress`) before reaching an `LLMServer` replica. The ingress replica proxies both the request and the streamed response, so each token in a streaming response crosses one extra deployment boundary. The ingress replica's event loop also handles both the inbound request path (which affects TTFT) and the outbound streamed-token path (which affects TPOT), so the two contend for the same loop under load.

**Direct streaming** removes that hop. When enabled, HAProxy forwards client traffic straight to the `LLMServer` replica that serves it, and an **ingress request router** chooses which model deployment and which replica handle each request. A small control-plane ingress stays behind to answer model discovery, but it's off the inference path entirely.

## Enable direct streaming

Direct streaming runs on top of the HAProxy ingress. Set both environment variables before starting Serve:

```bash
export RAY_SERVE_ENABLE_HA_PROXY=1
export RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING=1
```

Then build and deploy an application:

::::{tab-set}
:::{tab-item} Python
```{literalinclude} ../../../llm/doc_code/serve/direct_streaming/direct_streaming_example.py
:start-after: __direct_streaming_example_start__
:end-before: __direct_streaming_example_end__
:language: python
```
:::

:::{tab-item} YAML
```{literalinclude} ../../../llm/doc_code/serve/direct_streaming/direct_streaming_config.yaml
:language: yaml
```

Run `serve run` from a shell where both environment variables are still exported, so the controller enables HAProxy and direct streaming when it builds the application:

```bash
serve run config.yaml
```
:::
::::

The deployed application is OpenAI-compatible and exposes the engine's native routes, including `/v1/chat/completions`, `/v1/completions`, and `/v1/models`.

To confirm direct streaming is active, check that the application runs three kinds of deployment: one model deployment per model (`LLMServer:<model_id>`), a `DirectStreamingIngress` deployment, and an `LLMRouter` deployment. `DirectStreamingIngress` is the application's front door but serves only model discovery; `LLMRouter` is the ingress request router. Together they replace the standalone `OpenAiIngress` deployment that fronts a non-direct-streaming app and proxies every token.

Run `serve status`:

```bash
serve status
```

```yaml
applications:
  default:
    status: RUNNING
    deployments:
      DirectStreamingIngress:
        status: HEALTHY
        replica_states:
          RUNNING: 1
      LLMServer:qwen3_5-0_8b:
        status: HEALTHY
        replica_states:
          RUNNING: 1
      LLMRouter:
        status: HEALTHY
        replica_states:
          RUNNING: 1
```

The Serve dashboard shows the same deployments:

```{figure} ../images/direct_streaming_dashboard.png
---
width: 800px
name: direct-streaming-dashboard
---
The deployments appear healthy: the `LLMServer` model deployment, the `DirectStreamingIngress` control-plane ingress, and the `LLMRouter` ingress request router.
```

:::{tip}
Ray Serve sets `TCP_NODELAY` by default (`RAY_SERVE_HAPROXY_TCP_NODELAY=1`) so the first streamed chunk isn't held back by Nagle's algorithm. Keep it enabled for streaming workloads.
:::

## When to use direct streaming

Direct streaming is an experimental serving path for Ray Serve LLM. Removing the ingress proxy hop cuts per-token overhead on streaming responses, which matters most for long generations and latency-sensitive, high-throughput deployments. Ray intends it to become the default serving path as it matures.

## How it works

Without direct streaming, the request and every streamed token pass through the ingress deployment: `Client → HAProxy → OpenAiIngress replica → LLMServer replica → engine`.

With direct streaming, HAProxy calls `/internal/route` on the `LLMRouter` deployment for every request and gets back one deployment and one replica to send it to. Inference requests go straight to the chosen `LLMServer` replica, which serves the engine's own OpenAI-compatible FastAPI app, such as vLLM's API server, so no ingress deployment sits on the response path.

The application topology is:

```
DirectStreamingIngress          the app's front door; model discovery only
├── LLMServer:model-a           one deployment per model, reached directly
├── LLMServer:model-b
└── LLMRouter                   ingress request router
```

Routes split between the two paths, and never overlap:

- The routes `DirectStreamingIngress` declares — `GET /v1/models` and `GET /v1/models/{model}` — go to a `DirectStreamingIngress` replica. This deployment does nothing but answer model discovery; it never touches an inference request.
- Everything else, including `/v1/chat/completions` and `/v1/completions`, is routed by the request's `model` field to that model's `LLMServer` deployment and sent directly to one of its replicas.

Each model deployment gets its own HAProxy backend, so a retry or redispatch can never move a request onto another model's replicas.

```{figure} ../images/direct_streaming_architecture.png
---
width: 100%
name: direct-streaming-architecture
---
Direct streaming request path.
```

### Ingress request router

Ray Serve adds an internal router deployment that answers HAProxy's routing calls. For each request, HAProxy asks the router which replica to use over an internal endpoint, and the router returns that replica's backend host and port.

Replica selection reuses the `LLMServer` deployment's configured request router, so the same routing policies you would use for any deployment apply here. When you do not configure one, direct streaming defaults to `RoundRobinRouter`. This differs from Serve's general default of Power of Two Choices. You control it through the public `request_router_config`, described in {ref}`direct-streaming-customize`. The router deployment and its endpoint are internal and may change.

## Supported serving patterns

Direct streaming works with the OpenAI, data parallel attention, and prefill/decode builders:

- **Standard serving** (`build_openai_app`): supports **multiple models per application**, each as its own `LLMServer` deployment behind one `DirectStreamingIngress`. See [Serve multiple models](#direct-streaming-multi-model).
- **Data parallel attention** (`build_dp_openai_app`): single model. The `DPServer` deployment is itself the ingress and serves the engine app directly, with no separate control-plane ingress. Use this for wide expert parallelism. See {doc}`data-parallel-attention`.
- **Prefill/decode disaggregation** (`build_pd_openai_app`): single model. The decode server is the ingress and serves the engine app directly. See {doc}`prefill-decode`.

(direct-streaming-multi-model)=
## Serve multiple models

Pass several `LLMConfig`s to `build_openai_app` and each becomes its own `LLMServer` deployment. `GET /v1/models` lists all of them, and each inference request selects its model with the `model` field, exactly as on the default ingress.

Because the `model` field lives in the request body, multi-model routing requires body forwarding:

```bash
export RAY_SERVE_INGRESS_REQUEST_ROUTER_FORWARD_BODY=1
```

`build_openai_app` raises if you configure more than one model without it — the router would have nothing to select on, and every request would fail closed rather than land on an arbitrary model. See [Body-aware routers](#body-aware-routers) for what forwarding costs. A single-model application doesn't need it: with one model the `model` field is optional and requests that omit it still serve.

Requests that name no model, or name a model the application doesn't serve, fail closed rather than being sent somewhere arbitrary. See [Limitations](#direct-streaming-limitations) for how those errors currently reach the client.

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

Some policies score replicas using the request body, for example {ref}`prefix-aware routing <prefix-aware-routing-guide>`, which keys on the prompt or messages. By default HAProxy doesn't forward the request body to the router, because buffering and re-emitting large bodies adds time to first token (TTFT). Body-independent policies are unaffected. Round-robin and power of two ignore the body, and session-aware policies key on the header instead.

If your policy needs the body, enable forwarding:

```bash
export RAY_SERVE_INGRESS_REQUEST_ROUTER_FORWARD_BODY=1
```

With forwarding on, HAProxy has to receive and buffer the request body before it can route. That wait adds to TTFT. The more of the body it waits for, the longer routing is delayed and the more memory HAProxy holds. To bound that cost, HAProxy buffers only up to `RAY_SERVE_HAPROXY_INGRESS_REQUEST_ROUTER_BUFSIZE` bytes. When a request body is larger than that cap, HAProxy stops waiting and routes on the leading bytes it already has. It flags the routing call as carrying a truncated body, so the policy knows it's scoring against a prefix rather than the full payload.

Truncation affects only the copy sent to the router, not the request forwarded to the replica or the response to the client. The captured portion is always the head of the body, which is what prefix-based policies match on. To tune the cap against real traffic, watch the `serve_haproxy_ingress_router_truncations_total` metric. Enable the ingress request router metrics with `RAY_SERVE_INGRESS_REQUEST_ROUTER_METRICS_ENABLED=1`. A high truncation rate means body-aware policies are routing on clipped prompts and may warrant a larger buffer. See [HAProxy ingress request router metrics](../../monitoring.md#haproxy-ingress-request-router-metrics) for the full set.

### Session affinity

To pin all turns of a conversation to the same replica, send a session-id header with each request. HAProxy forwards the header to the ingress request router, which passes the session id to the configured policy. Session-aware policies such as `ConsistentHashRouter` then route every request with the same session id to one replica.

The header name defaults to `x-session-id` and is configurable with `RAY_SERVE_SESSION_ID_HEADER_KEY`. Matching is case-insensitive and tolerant of the `-`/`_` substitutions some proxies make.

(direct-streaming-limitations)=
## Limitations

- **Multiple models only on `build_openai_app`.** The data parallel attention and prefill/decode builders keep their single-model topology, where the server deployment is itself the ingress. They raise if you configure more than one model.
- **No multi-model KV-aware routing.** {ref}`KV-aware routing <kv-aware-routing-guide>` supports one model per application. Its token tracker is per-process and its pre-routing tokenizer is per-model, so `build_openai_app` rejects a multi-model application where any model requests it.
- **No multi-model LoRA.** `build_openai_app` rejects a multi-model application where any model sets `lora_config`. A single base model with adapters still serves and still lists its adapters under `GET /v1/models`.
- **No LoRA- or multiplex-aware routing.** The ingress request router doesn't steer requests to replicas that already have a given LoRA adapter loaded, and the default `RoundRobinRouter` is multiplex-unaware. A single base model with adapters serves, but without adapter affinity. If you need adapter-affinity routing, use the default ingress instead, which routes multiplex-aware. See [Multi-LoRA deployment](multi-lora.md). LoRA- and multiplex-aware routing for direct streaming is planned for a future release.
- **No custom control ingress or custom control routes.** `ingress_cls_config` is rejected while direct streaming is enabled. The routes the control ingress declares are exactly the routes taken away from the model deployments, so a custom ingress would silently pull inference traffic back through a Python hop.
- **The control ingress can't scale to zero.** It's the only deployment serving model discovery, and the ingress request router needs a running ingress replica to resolve its routes. `build_openai_app` rejects an `ingress_deployment_config` that sets `autoscaling_config.min_replicas` to `0`. Model deployments can still scale to zero.
- **Routing errors reach the client as 503.** HAProxy currently treats any non-200 from the ingress request router as a routing failure and answers `503` with `X-Serve-Reason: router_non_200`. A request with no `model` field on a multi-model application (router `400`) and a request naming an unknown model (router `404`) therefore both surface as `503` today. Forwarding the router's own status and OpenAI-shaped error body is planned.

## See also

- {ref}`routing-policies-guide` - request routing concepts and available policies
- {ref}`prefix-aware-routing-guide` - cache-locality routing policy
- {ref}`custom-request-router-guide` - implement a custom request router
