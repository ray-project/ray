# Serving Large Language Models with Ray LLM

This section helps you run large language models on Ray Serve by

* running LLMs with the `ray-llm` library
* querying LLMs running on `ray-llm`
* inspecting `ray-llm` applications

## LLM Background

Large language models (LLMs) are text-generation models with a huge number of weights. For example, recently released models like Falcon 40B or Llama 70B have tens of billions of parameters. Serving these often requires scarce, expensive hardware like NVidia A100s or H100s. Additionally, these models have high and variable latency when generating responses since each response token must be generated sequentially, and each response may contain a variable number of token.

The `ray-llm` library is built on top of Ray Serve and Ray Core. It leverages cutting-edge optimizations to serve LLMs efficiently and stably. The `ray-llm` library enables you to quickly develop production-ready LLM applications at low cost.

## Running LLMs

Install the `ray-llm` package by running

```
...
```

The `ray-llm` package runs and manages LLMs using the same Serve CLI as any other Serve app. For testing purposes, this tutorial uses the `...` LLM. This is a fake model that produces random text. It lets you test your `ray-llm` setup locally without needing a large GPU. See this page to learn what LLMs `ray-llm` supports.

Run the `...` model with `serve run`:

```
...
```

## Querying LLMs

Use this script to send requests to the `ray-llm` application:

```
...
```

## Inspecting LLMs

Use `serve status` to inspect LLM health in production:

```
...
```
