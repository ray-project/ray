(serving-llms)=

# Serving LLMs

Ray Serve LLM provides a high-performance, scalable framework for deploying Large Language Models (LLMs) in production. It specializes Ray Serve primitives for distributed LLM serving workloads, offering enterprise-grade features with OpenAI API compatibility.

## Why Ray Serve LLM?

Ray Serve LLM excels at highly distributed multi-node inference workloads:

- **Advanced parallelism strategies**: Seamlessly combine pipeline parallelism, tensor parallelism, expert parallelism, and data parallel attention for models of any size.
- **Prefill-decode disaggregation**: Separates and optimizes prefill and decode phases independently for better resource utilization and cost efficiency.
- **Custom request routing**: Implements prefix-aware, session-aware, or custom routing logic to maximize cache hits and reduce latency.
- **Multi-node deployments**: Serves massive models that span multiple nodes with automatic placement and coordination.
- **Production-ready**: Has built-in autoscaling, monitoring, fault tolerance, and observability.

## Features

- ⚡️ Automatic scaling and load balancing
- 🌐 Unified multi-node multi-model deployment
- 🔌 OpenAI-compatible API
- 🔄 Multi-LoRA support with shared base models
- 🚀 Engine-agnostic architecture (vLLM, SGLang, etc.)
- 📊 Built-in metrics and Grafana dashboards
- 🎯 Advanced serving patterns (PD disaggregation, data parallel attention)

## Install

Ray Serve LLM ships with Ray. Install it together with the serve and llm extras:

```bash
pip install "ray[serve,llm]"
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
