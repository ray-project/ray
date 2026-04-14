(sglang-integration)=
# SGLang integration

Ray Serve LLM provides an OpenAI-compatible API that integrates with [SGLang](https://docs.sglang.ai/) via the `server_cls` parameter on `LLMConfig`. Most `engine_kwargs` that work with `sglang serve` also work here, giving you SGLang's feature set through Ray Serve's distributed deployment capabilities.

The integration uses `SGLangServer`, a custom server class that wraps SGLang's in-process engine and exposes chat, completions, embeddings, tokenize, and detokenize endpoints through the standard Ray Serve LLM protocol.

This compatibility means you can:

- Use SGLang's RadixAttention and other optimizations with Ray Serve's production features
- Deploy SGLang models with autoscaling, multi-model serving, and advanced routing
- Serve models across multiple nodes with tensor and pipeline parallelism
- Run offline batch inference with SGLang through Ray Data

:::{note}
Community SGLang support is in early development. Track progress and provide feedback at [ray-project/ray#61114](https://github.com/ray-project/ray/issues/61114).
:::

## Prerequisites

```bash
pip install ray[serve,llm] "sglang[all,ray]"
```

Set the following environment variable before running any example:

- **CUDA:** `RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES=0`
- **ROCm:** `RAY_EXPERIMENTAL_NOSET_HIP_VISIBLE_DEVICES=0`

## Online serving (single node)

Deploy a single-node SGLang model with autoscaling. The `server_cls` parameter tells Ray Serve LLM to use the `SGLangServer` instead of the default vLLM engine.

::::{tab-set}

:::{tab-item} Server
:sync: server

```{literalinclude} ../../../llm/doc_code/serve/sglang/sglang_serving_example.py
:language: python
:start-after: __sglang_single_node_start__
:end-before: __sglang_single_node_end__
```
:::

:::{tab-item} Python Client
:sync: client

```{literalinclude} ../../../llm/doc_code/serve/sglang/sglang_query_example.py
:language: python
:start-after: __sglang_query_start__
:end-before: __sglang_query_end__
```
:::

:::{tab-item} cURL
:sync: curl

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
:::

::::

**Run:**

```bash
RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES=0 serve run serve_sglang_example:app
```

## Online serving (multi-node with TP+PP)

Deploy a large model across multiple nodes using tensor parallelism (TP=4) and pipeline parallelism (PP=2). This requires 2 nodes with 4 GPUs each (8 GPUs total).

The `placement_group_strategy: "PACK"` fills GPUs on each node before moving to the next, so with 2 nodes (4 GPUs each) each node gets one pipeline stage. The `SGLangServer.get_deployment_options()` method constructs placement groups from the `placement_group_config`.

::::{tab-set}

:::{tab-item} Python
:sync: python

```{literalinclude} ../../../llm/doc_code/serve/sglang/sglang_multinode_example.py
:language: python
:start-after: __sglang_multinode_start__
:end-before: __sglang_multinode_end__
```
:::

::::

**Run:**

```bash
RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES=0 serve run serve_sglang_multinode_example:app
```

## Batch inference

Run offline batch inference using `SGLangEngineProcessorConfig` from `ray.data.llm`. This integrates SGLang with Ray Data for processing large datasets.

::::{tab-set}

:::{tab-item} Python
:sync: python

```{literalinclude} ../../../llm/doc_code/serve/sglang/sglang_batch_example.py
:language: python
:start-after: __sglang_batch_start__
:end-before: __sglang_batch_end__
```
:::

::::

**Run:**

```bash
RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES=0 python batch_sglang_example.py
```

## Limitations

- **Engine replicas:** Not supported
- **Data parallelism (DP):** Requires a separate coordinator pattern; not supported in these examples
- **Transcriptions and score:** Not implemented in `SGLangServer`

## Dependencies

SGLang's in-process engine overrides Python signal handlers on startup. The `SGLangServer.__init__` includes a workaround that saves and restores signal handlers around engine initialization. If you encounter issues with graceful shutdown, this is a known area of friction.

## See also

- [SGLang supported models](https://docs.sglang.ai/supported_models/classify_models.html#supported-models)
- [SGLang OpenAI compatibility](https://docs.sglang.ai/basic_usage/openai_api.html)
- {doc}`../quick-start` - Basic LLM deployment examples
- {doc}`cross-node-parallelism` - Cross-node parallelism with placement groups
- [SGLang support tracking issue](https://github.com/ray-project/ray/issues/61114)
