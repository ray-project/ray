# SGLang on Ray Serve LLM

This directory contains example scripts for using SGLang with Ray Serve LLM.

For the full user guide, see the [SGLang integration documentation](https://docs.ray.io/en/latest/serve/llm/user-guides/sglang.html).

## Examples

| File | Description |
|------|-------------|
| `serve_sglang_example.py` | Single-node SGLang serving with autoscaling |
| `serve_sglang_multinode_example.py` | Multi-node serving with tensor and pipeline parallelism |
| `batch_sglang_example.py` | Batch inference using Ray Data |
| `query_example.py` | OpenAI client for querying a running deployment |

## Prerequisites

```bash
pip install ray[serve,llm] "sglang[all,ray]"
```

Set the environment variable before running:

- **CUDA:** `RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES=0`
- **ROCm:** `RAY_EXPERIMENTAL_NOSET_HIP_VISIBLE_DEVICES=0`

## Engine implementation

The `SGLangServer` class is located at `ray.llm._internal.serve.engines.sglang` and wraps SGLang's in-process engine with the Ray Serve LLM server protocol.
