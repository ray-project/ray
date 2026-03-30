# SGLang on Ray Serve LLM

:::{warning}
This directory is a **demonstration and reference only**. It is not actively maintained and is not part of Ray's officially supported feature set. Use it as a starting point for your own implementations.
:::

:::{note}
Community SGLang support is in early development. Track progress and provide feedback at [ray-project/ray#61114](https://github.com/ray-project/ray/issues/61114).
:::

## Overview

Ray Serve LLM provides an OpenAI-compatible API that integrates with SGLang via the `server_cls` parameter on `LLMConfig`. Most `engine_kwargs` that work with `sglang serve` also work here, giving you SGLang's feature set through Ray Serve's distributed deployment capabilities.

The integration uses a custom `SGLangServer` class (in `modules/sglang_engine.py`) that wraps SGLang's in-process engine and exposes chat, completions, embeddings, tokenize, and detokenize endpoints.

## Prerequisites

```bash
pip install ray[serve,llm] "sglang[all]"
```

Set the following environment variable before running any example:

- **CUDA:** `RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES=0`
- **ROCm:** `RAY_EXPERIMENTAL_NOSET_HIP_VISIBLE_DEVICES=0`

## Examples

### 1. Online Serving (Single Node)

Deploys a single-node SGLang model with autoscaling.

**File:** `serve_sglang_example.py`

```{literalinclude} ../../../../../python/ray/llm/examples/sglang/serve_sglang_example.py
:language: python
```

**Run:**

```bash
RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES=0 serve run serve_sglang_example:app
```

### 2. Online Serving (Multi-Node TP+PP)

Deploys a large model across multiple nodes using tensor parallelism (TP=4) and pipeline parallelism (PP=2). Requires 2 nodes with 4 GPUs each (8 GPUs total).

**File:** `serve_sglang_multinode_example.py`

```{literalinclude} ../../../../../python/ray/llm/examples/sglang/serve_sglang_multinode_example.py
:language: python
```

**Run:**

```bash
RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES=0 serve run serve_sglang_multinode_example:app
```

The `placement_group_strategy: "SPREAD"` distributes GPU bundles across nodes. The `SGLangServer.get_deployment_options()` method constructs placement groups from the `placement_group_config`.

### 3. Batch Inference

Runs offline batch inference using `SGLangEngineProcessorConfig` from `ray.data.llm`.

**File:** `batch_sglang_example.py`

```{literalinclude} ../../../../../python/ray/llm/examples/sglang/batch_sglang_example.py
:language: python
```

**Run:**

```bash
RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES=0 python batch_sglang_example.py
```

### 4. Query Client

Queries a running SGLang deployment using the OpenAI Python SDK. Start one of the serving examples first.

**File:** `query_example.py`

```{literalinclude} ../../../../../python/ray/llm/examples/sglang/query_example.py
:language: python
```

**Run:**

```bash
python query_example.py
```

**cURL alternative:**

```bash
# Chat completions
curl http://localhost:8000/v1/chat/completions \
    -H "Content-Type: application/json" \
    -d '{
        "model": "Llama-3.1-8B-Instruct",
        "messages": [{"role": "user", "content": "List 3 countries and their capitals."}],
        "temperature": 0,
        "max_tokens": 64
    }'

# Text completions
curl http://localhost:8000/v1/completions \
    -H "Content-Type: application/json" \
    -d '{
        "model": "Llama-3.1-8B-Instruct",
        "prompt": "San Francisco is a",
        "max_tokens": 30,
        "temperature": 0
    }'
```

## Limitations

- **Engine replicas:** Not supported
- **Data parallelism (DP):** Requires a separate coordinator pattern; not supported in these examples
- **Transcriptions and score:** Not implemented in `SGLangServer`

## Dependencies

SGLang's in-process engine overrides Python signal handlers on startup. The `SGLangServer.__init__` includes a workaround that saves and restores signal handlers around engine initialization. If you encounter issues with graceful shutdown, this is a known area of friction.

## See Also

- [SGLang supported models](https://docs.sglang.ai/supported_models/classify_models.html#supported-models)
- [SGLang OpenAI compatibility](https://docs.sglang.ai/basic_usage/openai_api.html)
- [Ray Serve LLM documentation](https://docs.ray.io/en/latest/serve/llm/serving-llms.html)
- [Ray Data batch inference](https://docs.ray.io/en/latest/data/batch-inference.html)
- [SGLang support tracking issue](https://github.com/ray-project/ray/issues/61114)
