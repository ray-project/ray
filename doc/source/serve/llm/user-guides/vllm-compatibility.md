(vllm-compatibility-guide)=
# vLLM compatibility

Ray Serve LLM provides an OpenAI-compatible API that aligns with vLLM's OpenAI-compatible server. Most of the `engine_kwargs` that work with `vllm serve` work with Ray Serve LLM, giving you access to vLLM's feature set through Ray Serve's distributed deployment capabilities.

This compatibility means you can:

- Use the same model configurations and engine arguments as vLLM
- Leverage vLLM's latest features (multimodal, structured output, reasoning models)
- Switch between `vllm serve` and Ray Serve LLM with no code changes and scale
- Take advantage of Ray Serve's production features (autoscaling, multi-model serving, advanced routing)

This guide shows how to use vLLM features such as embeddings, structured output, vision language models, and reasoning models with Ray Serve.

## Embeddings

You can generate embeddings by setting the `task` parameter to `"embed"` in the engine arguments. Models supporting this use case are listed in the [vLLM text embedding models documentation](https://docs.vllm.ai/en/stable/models/supported_models.html#text-embedding-task-embed).


### Deploy an embedding model

::::{tab-set}

:::{tab-item} Server
:sync: server

```python
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="qwen-0.5b",
        model_source="Qwen/Qwen2.5-0.5B-Instruct",
    ),
    engine_kwargs=dict(
        task="embed",
    ),
)

app = build_openai_app({"llm_configs": [llm_config]})
serve.run(app, blocking=True)
```
:::

:::{tab-item} Python Client
:sync: client

```python
from openai import OpenAI

# Initialize client
client = OpenAI(base_url="http://localhost:8000/v1", api_key="fake-key")

# Generate embeddings
response = client.embeddings.create(
    model="qwen-0.5b",
    input=["A text to embed", "Another text to embed"],
)

for data in response.data:
    print(data.embedding)  # List of float of len 4096
```
:::

:::{tab-item} cURL
:sync: curl

```bash
curl -X POST http://localhost:8000/v1/embeddings \
     -H "Content-Type: application/json" \
     -H "Authorization: Bearer fake-key" \
     -d '{
           "model": "qwen-0.5b",
           "input": ["A text to embed", "Another text to embed"],
           "encoding_format": "float"
         }'
```
:::

::::


## Transcriptions

You can generate audio transcriptions using Speech-to-Text (STT) models trained specifically for Automatic Speech Recognition (ASR) tasks. Models supporting this use case are listed in the [vLLM transcription models documentation](https://docs.vllm.ai/en/stable/models/supported_models.html).


### Deploy a transcription model

::::{tab-set}

:::{tab-item} Server
:sync: server

```{literalinclude} ../../../llm/doc_code/serve/transcription/transcription_example.py
:language: python
:start-after: __transcription_example_start__
:end-before: __transcription_example_end__
```
:::

:::{tab-item} Python Client
:sync: client

```python
from openai import OpenAI

# Initialize client
client = OpenAI(base_url="http://localhost:8000/v1", api_key="fake-key")

# Open audio file
with open("/path/to/audio.wav", "rb") as f:
    # Make a request to the transcription model
    response = client.audio.transcriptions.create(
        model="whisper-large",
        file=f,
        temperature=0.0,
        language="en",
    )

    print(response.text)
```
:::

:::{tab-item} cURL
:sync: curl

```bash
curl http://localhost:8000/v1/audio/transcriptions \
    -X POST \
    -H "Authorization: Bearer fake-key" \
    -F "file=@/path/to/audio.wav" \
    -F "model=whisper-large" \
    -F "temperature=0.0" \
    -F "language=en"
```
:::

::::


## Structured output

You can request structured JSON output similar to OpenAI's API using JSON mode or JSON schema validation with Pydantic models.

### JSON mode

::::{tab-set}

:::{tab-item} Server
:sync: server

```python
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="qwen-0.5b",
        model_source="Qwen/Qwen2.5-0.5B-Instruct",
    ),
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1,
            max_replicas=2,
        )
    ),
    accelerator_type="A10G",
)

# Build and deploy the model
app = build_openai_app({"llm_configs": [llm_config]})
serve.run(app, blocking=True)
```
:::

:::{tab-item} Client (JSON Object)
:sync: client

```python
from openai import OpenAI

# Initialize client
client = OpenAI(base_url="http://localhost:8000/v1", api_key="fake-key")

# Request structured JSON output
response = client.chat.completions.create(
    model="qwen-0.5b",
    response_format={"type": "json_object"},
    messages=[
        {
            "role": "system",
            "content": "You are a helpful assistant that outputs JSON."
        },
        {
            "role": "user",
            "content": "List three colors in JSON format"
        }
    ],
    stream=True,
)

for chunk in response:
    if chunk.choices[0].delta.content is not None:
        print(chunk.choices[0].delta.content, end="", flush=True)
# Example response:
# {
#   "colors": [
#     "red",
#     "blue",
#     "green"
#   ]
# }
```
:::

::::

### JSON schema with Pydantic

You can specify the exact schema you want for the response using Pydantic models:

```python
from openai import OpenAI
from typing import List, Literal
from pydantic import BaseModel

# Initialize client
client = OpenAI(base_url="http://localhost:8000/v1", api_key="fake-key")

# Define a pydantic model of a preset of allowed colors
class Color(BaseModel):
    colors: List[Literal["cyan", "magenta", "yellow"]]

# Request structured JSON output
response = client.chat.completions.create(
    model="qwen-0.5b",
    response_format={
        "type": "json_schema",
        "json_schema": Color.model_json_schema()
    },
    messages=[
        {
            "role": "system",
            "content": "You are a helpful assistant that outputs JSON."
        },
        {
            "role": "user",
            "content": "List three colors in JSON format"
        }
    ],
    stream=True,
)

for chunk in response:
    if chunk.choices[0].delta.content is not None:
        print(chunk.choices[0].delta.content, end="", flush=True)
# Example response:
# {
#   "colors": [
#     "cyan",
#     "magenta",
#     "yellow"
#   ]
# }
```

## Vision language models

You can deploy multimodal models that process both text and images. Ray Serve LLM supports vision models through vLLM's multimodal capabilities.

### Deploy a vision model

::::{tab-set}

:::{tab-item} Server
:sync: server

```python
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app


# Configure a vision model
llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="pixtral-12b",
        model_source="mistral-community/pixtral-12b",
    ),
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1,
            max_replicas=2,
        )
    ),
    accelerator_type="L40S",
    engine_kwargs=dict(
        tensor_parallel_size=1,
        max_model_len=8192,
    ),
)

# Build and deploy the model
app = build_openai_app({"llm_configs": [llm_config]})
serve.run(app, blocking=True)
```
:::

:::{tab-item} Client
:sync: client

```python
from openai import OpenAI

# Initialize client
client = OpenAI(base_url="http://localhost:8000/v1", api_key="fake-key")

# Create and send a request with an image
response = client.chat.completions.create(
    model="pixtral-12b",
    messages=[
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": "What's in this image?"
                },
                {
                    "type": "image_url",
                    "image_url": {
                        "url": "https://example.com/image.jpg"
                    }
                }
            ]
        }
    ],
    stream=True,
)

for chunk in response:
    if chunk.choices[0].delta.content is not None:
        print(chunk.choices[0].delta.content, end="", flush=True)
```
:::

::::

### Supported models

For a complete list of supported vision models, see the [vLLM multimodal models documentation](https://docs.vllm.ai/en/stable/models/supported_models.html#multimodal-language-models).

## Reasoning models

Ray Serve LLM supports reasoning models such as DeepSeek-R1 and QwQ through vLLM. These models use extended thinking processes before generating final responses.

For reasoning model support and configuration, see the [vLLM reasoning models documentation](https://docs.vllm.ai/en/stable/models/supported_models.html).

## See also

- [vLLM supported models](https://docs.vllm.ai/en/stable/models/supported_models.html) - Complete list of supported models and features
- [vLLM OpenAI compatibility](https://docs.vllm.ai/en/stable/serving/openai_compatible_server.html) - vLLM's OpenAI-compatible server documentation
- {doc}`Quickstart <../quick-start>` - Basic LLM deployment examples

