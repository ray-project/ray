---
myst:
  html_meta:
    description: "Inspect or block OpenAI-compatible LLM traffic with GovernanceIngress, LLMMiddleware, and build_openai_app's ingress_cls_config."
---

(serve-llm-governance)=

# Governance middleware

Inspect or block OpenAI-compatible chat, completions, and transcription requests on the Serve LLM ingress. Subclass {class}`~ray.serve.llm.governance.LLMMiddleware`, then pass {class}`~ray.serve.llm.governance.GovernanceIngress` to {func}`~ray.serve.llm.build_openai_app` through `ingress_cls_config`.

:::{note}
This API is in alpha and may change before becoming stable.
:::

## Deploy with a middleware

The following example denies one model ID before inference. `before_inference` is the only required hook. Return the request to proceed, or a {class}`~ray.serve.llm.governance.BlockedResponse` to stop.

```python
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app
from ray.serve.llm.governance import (
    BlockedResponse,
    GovernanceIngress,
    LLMMiddleware,
)

class DenyModelMiddleware(LLMMiddleware):
    async def before_inference(self, request, context):
        if context.model_id == "blocked-model":
            return BlockedResponse(
                rule_triggered="ACCESS_DENIED",
                reason="Model is not allowed",
            )
        return request

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="qwen-0.5b",
        model_source="Qwen/Qwen2.5-0.5B-Instruct",
    ),
)

app = build_openai_app(
    {
        "llm_configs": [llm_config],
        "ingress_cls_config": {
            "ingress_cls": GovernanceIngress,
            "ingress_extra_kwargs": {
                "middlewares": [DenyModelMiddleware()],
            },
        },
    }
)
serve.run(app, blocking=True)
```

`ingress_cls` is the ingress class. Pass the class object, or an import path such as `ray.serve.llm.governance:GovernanceIngress`. `ingress_extra_kwargs` is forwarded to that class's constructor. `GovernanceIngress` reads `middlewares`, a list of `LLMMiddleware` instances, and runs them in list order. `build_openai_app` validates the argument as {class}`~ray.serve.llm.LLMServingArgs`.

## What hooks run?

`GovernanceIngress` wraps the chat, completions, and transcription paths. Embeddings and scoring don't run these hooks.

Each middleware can implement three hooks:

| Hook | When it runs | Return |
|------|----------------|--------|
| `before_inference(request, context)` | Before the model replica is called. Required. | The request, which you may modify, or a `BlockedResponse`. |
| `after_inference(request, response, context)` | After inference, on the full non-streaming response or the first stream chunk. Optional. The default passes the response through. | The response, which you may modify, or a `BlockedResponse`. |
| `on_inference_complete(usage, context)` | After the response is returned, including after a stream ends. Optional. The default does nothing. | `None`. `usage` is a dict taken from the response usage field. |

When you pass more than one middleware, the chain runs them in list order. The first `BlockedResponse` stops the chain. Put cheap checks such as access control and budget before expensive ones such as PII scanning.

On a streaming response, `after_inference` sees the first chunk only. `on_inference_complete` still runs after the stream ends, with the last usage dict the stream produced.

## How context and blocks work

{class}`~ray.serve.llm.governance.RequestContext` is built from the request body and the HTTP request:

| Field | Source |
|-------|--------|
| `model_id` | The `model` field on the body |
| `request_id` | Serve request state, or the `x-request-id` header |
| `session_id` | Derived from request headers |
| `max_tokens` | `max_tokens` or `max_completion_tokens` on the body |
| `user_id` | Serve request state, the body `user` field, or the `x-user-id` header |
| `tenant_id` | The `x-tenant-id` header |
| `headers` | A copy of the inbound HTTP headers |

Return `BlockedResponse` from `before_inference` or `after_inference` to stop the request. The ingress maps it to an OpenAI-style JSON error:

```json
{
  "error": {
    "message": "Model is not allowed",
    "type": "ACCESS_DENIED",
    "code": "ACCESS_DENIED"
  }
}
```

HTTP status comes from `decision` and `rule_triggered`:

| Condition | Status |
|-----------|--------|
| `decision="THROTTLED"` | 429, with a `Retry-After` header |
| `rule_triggered="PII_DETECTED"` | 400 |
| `rule_triggered="BUDGET_EXCEEDED"` | 402 |
| `rule_triggered="ACCESS_DENIED"`, or any other blocked rule | 403 |

A throttled block requires `retry_after`.

## Where this does not apply

`GovernanceIngress` replaces the default `OpenAiIngress` deployment. Direct streaming uses the LLM server itself as the HTTP ingress, so `build_openai_app` rejects a custom `ingress_cls_config`.

:::{warning}
Do not set `RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING` when you use `GovernanceIngress`.
:::

## See also

- {class}`~ray.serve.llm.governance.GovernanceIngress`, {class}`~ray.serve.llm.governance.LLMMiddleware`, {class}`~ray.serve.llm.governance.RequestContext`, and {class}`~ray.serve.llm.governance.BlockedResponse` in the {ref}`LLM API reference <serve-llm-api>`
- {doc}`Direct streaming <direct-streaming>` for the ingress path this feature doesn't cover
- {doc}`Quickstart <../quick-start>` for a deployment without middleware
