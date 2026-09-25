---
myst:
  html_meta:
    description: "Point Claude Code at a Ray Serve LLM application by enabling direct streaming, configuring tool calling, and pinning the served model."
---

(claude-code-guide)=
# Use Ray Serve LLM with Claude Code

Claude Code calls the Anthropic Messages API. Ray Serve LLM serves that API only when direct streaming is on. This page deploys one model and points Claude Code at it.

## Enable direct streaming

Claude Code requires direct streaming. See {doc}`Direct streaming <direct-streaming>`.

Set both environment variables before you start Serve:

```bash
export RAY_SERVE_ENABLE_HA_PROXY=1
export RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING=1
```

Run `serve run` from a shell where both variables are still exported, so the controller enables HAProxy and direct streaming when it builds the application.

## Configure tool calling

Anthropic tool calling needs `engine_kwargs` that match the model you serve. Follow the [vLLM tool calling guide](https://docs.vllm.ai/en/stable/features/tool_calling/) and use the `tool_call_parser` that guide documents for your model.

The following `engine_kwargs` are an example for `Qwen/Qwen3.5-0.8B`. The config in the next section includes the same values.

```yaml
engine_kwargs:
  enable_auto_tool_choice: true
  tool_call_parser: qwen3_coder
  reasoning_parser: qwen3
```

## Deploy the application

Save the following config as `config.yaml`:

```{literalinclude} ../../../llm/doc_code/serve/direct_streaming/direct_streaming_config.yaml
:language: yaml
```

Then start the application:

```bash
serve run config.yaml
```

Claude Code sends a model name with each request. That name must match `model_loading_config.model_id`, not `model_source`. For this config, pass `qwen3.5-0.8b`, not `Qwen/Qwen3.5-0.8B`.

## Point Claude Code at the application

In the shell where you run Claude Code, set the following variables. Replace `<serve-host>` with the address of the node that runs Serve.

```bash
export ANTHROPIC_BASE_URL=http://<serve-host>:8000
export ANTHROPIC_API_KEY=not-needed
export ANTHROPIC_DEFAULT_HAIKU_MODEL=qwen3.5-0.8b
claude --model qwen3.5-0.8b
```

`--model` sets the model for the main session. Claude Code sends background requests with its configured Haiku model. Pin `ANTHROPIC_DEFAULT_HAIKU_MODEL` to the only model this application serves, or those requests fail with a model-not-found error.

If you configure a subagent with its own model, set that model to `qwen3.5-0.8b` too. Otherwise those requests fail with a model-not-found error.

Claude Code requires a credential even when the local endpoint doesn't check authentication. Any non-empty `ANTHROPIC_API_KEY` works.

## Limitations

Direct streaming serves one model per application and doesn't route by LoRA adapter. See {ref}`direct-streaming-limitations`.
