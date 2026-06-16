(serving-llms)=

# Serving LLMs

Ray Serve LLM deploys large language models in production. It builds on Ray Serve primitives for distributed, multi-node LLM serving and exposes an OpenAI-compatible API.

## Key features

- OpenAI-compatible API for chat, completions, and embeddings.
- Multi-node, multi-model deployment with autoscaling and load balancing.
- Parallelism strategies: tensor, pipeline, expert, and data parallel attention.
- Prefill-decode disaggregation to scale the prefill and decode phases independently.
- Custom request routing, including prefix-aware routing for higher cache hit rates.
- Multi-LoRA serving on a shared base model.
- Engine-agnostic backends such as vLLM and SGLang.
- Built-in metrics and Grafana dashboards.

## Install

Ray Serve LLM ships with Ray. Install it with the `llm` extra:

```bash
pip install "ray[llm]"
```

This pulls in vLLM and the OpenAI-compatible server stack. You need a GPU to run most models. The {doc}`Quickstart <quick-start>` covers prerequisites, supported hardware, and gated-model setup.

## Deploy your first model

Define an {class}`~ray.serve.llm.LLMConfig`, build an OpenAI-compatible app, and run it:

```{literalinclude} ../../llm/doc_code/serve/qwen/qwen_example.py
:language: python
:start-after: __qwen_example_start__
:end-before: __qwen_example_end__
```

Once it is running, query it with any OpenAI client at `http://localhost:8000/v1`. See the {doc}`Quickstart <quick-start>` for client snippets, multi-model apps, and config-driven (YAML) deployments.

## Find your path

- **New here?** Start with the {doc}`Quickstart <quick-start>` to deploy and query a model.
- **Configuring a deployment?** The {doc}`Configuration reference <user-guides/configuration>` explains every `LLMConfig` field.
- **Scaling up?** The {doc}`User guides <user-guides/index>` cover parallelism, routing, caching, LoRA, and observability.
- **Want the internals?** The {doc}`Architecture <architecture/index>` docs explain components, request flow, and serving patterns.
- **Deploying a specific model?** The {doc}`Examples <examples>` walk through small, medium, large, vision, and reasoning models end to end.
- **Hitting an issue?** Check {doc}`Troubleshooting <troubleshooting>` and {doc}`Benchmarks <benchmarks>`.

```{toctree}
:hidden:

Quickstart <quick-start>
Examples <examples>
User Guides <user-guides/index>
Architecture <architecture/index>
Benchmarks <benchmarks>
Troubleshooting <troubleshooting>
```
