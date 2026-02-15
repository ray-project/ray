# Deploy with SGLang

:::{warning}
This is a community-supported example and isn't in active maintenance. Use it as a reference for your own implementations.
:::

Ray Serve LLM provides an OpenAI-compatible API that aligns with SGLang's OpenAI-compatible server. Most of the `engine_kwargs` that work with `sglang serve` work with Ray Serve LLM, giving you access to SGLang's feature set through Ray Serve's distributed deployment capabilities.

This compatibility means you can:

- Use the same model configurations and engine arguments as SGLang
- Leverage SGLang's latest features supported by SGLang's ServerArgs
- Switch between `sglang serve` and Ray Serve LLM with minimal code changes

What is NOT supported in this implementation:

- Engine replicas
- Embeddings, transcriptions, and score methods (only chat and completions are implemented)
- TP/PP/DP

## Core components

Your custom deployment consists of two main components:
1. **`SGLangServer`**: The core Ray Serve deployment that initializes the SGLang runtime and model. This deployment contains the model's business logic, including resource requirements and generation methods (`completions` and `chat_completions`).
2. **Environment variable**: Set `RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES=1` on CUDA devices or `RAY_EXPERIMENTAL_NOSET_HIP_VISIBLE_DEVICES=1` on ROCm devices.

## Deploy an LLM model using SGLang engine

Server:

```{literalinclude} ../../../../../../python/ray/llm/examples/sglang/serve_sglang_example.py
:language: python
```

Python client for the `v1/chat/completions` endpoint:

```python
import openai

client = openai.Client(base_url="http://127.0.0.1:8000/v1", api_key="None")

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

cURL client for the `v1/completions` endpoint:

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
