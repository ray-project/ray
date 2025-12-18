# SGLang Compatibility

:::{warning}
This is an example only and isn't in active maintenance. Use it as a reference for your own implementations.
:::

Ray Serve LLM provides an OpenAI-compatible API that aligns with SGLang's OpenAI-compatible server. Most of the `engine_kwargs` that work with `sglang serve` work with Ray Serve LLM, giving you access to SGLang's feature set through Ray Serve's distributed deployment capabilities.

This compatibility means you can:

- Use the same model configurations and engine arguments as SGLang
- Leverage SGLang's latest features supported by SGLang's ServerArgs
- Switch between `sglang serve` and Ray Serve LLM with no code changes and scale

This document provides a guide for deploying and interacting with the custom **SGLang engine** wrapped within a Ray Serve infrastructure. This setup leverages SGLang's efficient batching capabilities while providing a scalable, production-ready **OpenAI-compatible API**.

What is NOT supported in this implementation:

- Support for engine replicas
- SGLangServer implemented chat and completions methods but no embeddings, transcriptions, and score methods
- Support for TP/PP/DP

## Core Components

Your custom deployment consists of two main components:
1. `SGLangServer` (**Backend**): The core Ray Serve deployment that initializes the SGLang runtime and model. This deployment contains the model's business logic, including resource requirements and generation methods (`completions` and `chat_completions`).
2. enable ENV-VAR: `RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES=0` on CUDA device or `RAY_EXPERIMENTAL_NOSET_HIP_VISIBLE_DEVICES=0` on ROCm device
### Deploy an LLM model using SGLang Engine



Server

```{literalinclude} ../../../../../python/ray/llm/examples/sglang/serve_sglang.py
:language: python
```

Python Client
client for v1/chat/completions endpoint
```python
import openai

client = openai.Client(base_url=f"http://127.0.0.1:8000/v1", api_key="None")

response = client.chat.completions.create(
    model="Llama-3.1-8B-Instruct",
    messages=[
        {"role": "user", "content": "List 3 countries and their capitals."},
    ],
    temperature=0,
    max_tokens=64,
)

print(f"Response: {response}")
```

cURL
client for v1/completions endpoint
```bash
curl http://127.0.0.1:8000/v1/completions \
    -H "Content-Type: application/json" \
    -d '{
        "model": "Llama-3.1-8B-Instruct",
        "prompt": "San Francisco is a",
        "max_tokens": 30,
        "temperature": 0
    }'
```



## See also

- [SGLang supported models](https://docs.sglang.ai/supported_models/classify_models.html#supported-models) - Complete list of supported models and features
- [SGLang OpenAI compatibility](https://docs.sglang.ai/basic_usage/openai_api.html) - SGLang's OpenAI-compatible server documentation
- [Ray Serve LLM + SGLang PRD](https://docs.google.com/document/d/1FLaormOPANCCLBI_UBJLwPl6a9njIyT-kSJfhxLERXA/edit?usp=sharing) - On going working doc for Ray Serve LLM + SGLang PRD
