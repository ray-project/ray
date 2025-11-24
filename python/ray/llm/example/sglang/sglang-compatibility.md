# SGLang Compatibility

Ray Serve LLM provides an OpenAI-compatible API that aligns with SGLang's OpenAI-compatible server. Most of the `engine_kwargs` that work with `sglang serve` work with Ray Serve LLM, giving you access to SGLang's feature set through Ray Serve's distributed deployment capabilities.

This compatibility means you can:

- Use the same model configurations and engine arguments as SGLang
- Leverage SGLang's latest features supported by SGLang's ServerArgs
- Switch between `sglang serve` and Ray Serve LLM with no code changes and scale

This document provides a guide for deploying and interacting with the custom **SGLang engine** wrapped within a Ray Serve infrastructure. This setup leverages SGLang's efficient batching capabilities while providing a scalable, production-ready **OpenAI-compatible API**.

## Core Components

Your custom deployment consists of two main components:
1. `SGLangServer` (**Backend**): The core Ray Serve deployment that initializes the SGLang runtime and model. This deployment contains the model's business logic, including resource requirements and generation methods (`completions` and `chat_completions`).
2. enable ENV-VAR: `RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES=0` on CUDA device or `RAY_EXPERIMENTAL_NOSET_HIP_VISIBLE_DEVICES=0` on ROCm device
### Deploy an LLM model using SGLang Engine



Server
```python
from ray import serve
from ray.serve.llm import LLMConfig
from ray.llm._internal.serve.core.ingress.builder import build_sglang_openai_app

llm_config = LLMConfig(
    model_loading_config={
        "model_id": "Llama-3.1-8B-Instruct",
        "model_source": "unsloth/Llama-3.1-8B-Instruct",
    },
    deployment_config={
        "autoscaling_config": {
            "min_replicas": 1,
            "max_replicas": 2,
        }
    },
    llm_engine = 'SGLang',
    # Pass the desired accelerator type (e.g. A10G, L4, etc.)
    accelerator_type="H100",
    # for ROCm device ex: MI300X uses: "AMD-Instinct-MI300X-OAM",
    # You can customize the engine arguments (e.g. SGLang engine kwargs)
    engine_kwargs={
        "trust_remote_code": True,
        "model_path": "unsloth/Llama-3.1-8B-Instruct",
        "tp_size": 1,
        "mem_fraction_static": 0.8,
    },
)

app = build_sglang_openai_app({"llm_configs": [llm_config]})
serve.start()
serve.run(app, blocking=True)

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
