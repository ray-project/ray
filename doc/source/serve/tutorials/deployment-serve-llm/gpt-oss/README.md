---
orphan: true
---

<!--
Do not modify this README. This file is a copy of the notebook and is not used to display the content.
Modify notebook.ipynb instead, then regenerate this file with:
jupyter nbconvert "$notebook.ipynb" --to markdown --output "README.md"
-->

# Deploy gpt-oss

<div align="left">
<a target="_blank" href="https://console.anyscale.com/template-preview/deployment-serve-llm?file=%252Ffiles%252Fgpt-oss"><img src="https://img.shields.io/badge/🚀 Run_on-Anyscale-9hf"></a>&nbsp;
<a href="https://github.com/ray-project/ray/tree/master/doc/source/serve/tutorials/deployment-serve-llm/gpt-oss" role="button"><img src="https://img.shields.io/static/v1?label=&amp;message=View%20On%20GitHub&amp;color=586069&amp;logo=github&amp;labelColor=2f363d"></a>&nbsp;
</div>

*gpt-oss* is a family of open-source models designed for general-purpose language understanding and generation. The 20&nbsp;B parameter variant (`gpt-oss-20b`) offers strong reasoning capabilities with lower latency. This makes it well-suited for local or specialized use cases. The larger 120&nbsp;B parameter variant (`gpt-oss-120b`) is designed for production-scale, high-reasoning workloads.

For more information, see the [gpt-oss collection](https://huggingface.co/collections/openai/gpt-oss-68911959590a1634ba11c7a4).

---

## Configure Ray Serve LLM

Ray Serve LLM provides multiple [Python APIs](https://docs.ray.io/en/latest/serve/api/index.html#llm-api) for defining your application. Use [`build_openai_app`](https://docs.ray.io/en/latest/serve/api/doc/ray.serve.llm.build_openai_app.html#ray.serve.llm.build_openai_app) to build a full application from your [`LLMConfig`](https://docs.ray.io/en/latest/serve/api/doc/ray.serve.llm.LLMConfig.html#ray.serve.llm.LLMConfig) object.

Below are example configurations for both gpt-oss-20b and gpt-oss-120b, depending on your hardware and use case.

---

### gpt-oss-20b

To deploy a small-sized model such as gpt-oss-20b, a single GPU is sufficient:


```python
# serve_gpt_oss.py
from ray.serve.llm import LLMConfig, build_openai_app

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="my-gpt-oss",
        model_source="openai/gpt-oss-20b",
    ),
    accelerator_type="L4",
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1,
            max_replicas=2,
        )
    ),
    engine_kwargs=dict(
        max_model_len=32768
    ),
)

app = build_openai_app({"llm_configs": [llm_config]})
```


---

### gpt-oss-120b

To deploy a medium-sized model such as `gpt-oss-120b`, a single node with multiple GPUs is sufficient. Set `tensor_parallel_size` to distribute the model’s weights across the GPUs in your instance:


```python
# serve_gpt_oss.py
from ray.serve.llm import LLMConfig, build_openai_app

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="my-gpt-oss",
        model_source="openai/gpt-oss-120b",
    ),
    accelerator_type="L40S", # Or "A100-40G"
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1,
            max_replicas=2,
        )
    ),
    engine_kwargs=dict(
        max_model_len=32768,
        tensor_parallel_size=2,
    ),
)

app = build_openai_app({"llm_configs": [llm_config]})
```

**Note:** Before moving to a production setup, migrate to using a [Serve config file](https://docs.ray.io/en/latest/serve/production-guide/config.html) to make your deployment version-controlled, reproducible, and easier to maintain for CI/CD pipelines. For an example, see [Serving LLMs - Quickstart Examples: Production Guide](https://docs.ray.io/en/latest/serve/llm/quick-start.html#production-deployment).

---

## Deploy locally

### Prerequisites

* Access to GPU compute.

### Dependencies

gpt-oss integration is available starting from `ray>=2.49.0` and `vllm==0.10.1`.

```bash
pip install "ray[serve,llm]>=2.49.0"
pip install "vllm==0.10.1"
```

---

### Launch the service

Follow the instructions in [Configure Ray Serve LLM](#configure-ray-serve-llm) according to the model size you choose, and define your app in a Python module `serve_gpt_oss.py`.

In a terminal, run:


```python
serve run serve_gpt_oss:app --non-blocking
```

Deployment typically takes a few minutes as Ray provisions the cluster, the vLLM server starts, and Ray Serve downloads the model.

---

### Send requests

Your endpoint is available locally at `http://localhost:8000`. You can use a placeholder authentication token for the OpenAI client, for example `"FAKE_KEY"`.

#### Example curl


```python
curl -X POST http://localhost:8000/v1/chat/completions \
  -H "Authorization: Bearer FAKE_KEY" \
  -H "Content-Type: application/json" \
  -d '{ "model": "my-gpt-oss", "messages": [{"role": "user", "content": "How many Rs in strawberry ?"}] }'
```

#### Example Python


```python
#client.py
from urllib.parse import urljoin
from openai import OpenAI

api_key = "FAKE_KEY"
base_url = "http://localhost:8000"

client = OpenAI(base_url=urljoin(base_url, "v1"), api_key=api_key)

# Example query
response = client.chat.completions.create(
    model="my-gpt-oss",
    messages=[
        {"role": "user", "content": "How many r's in strawberry"}
    ],
    stream=True
)

# Stream
for chunk in response:
    # Stream reasoning content
    if hasattr(chunk.choices[0].delta, "reasoning_content"):
        data_reasoning = chunk.choices[0].delta.reasoning_content
        if data_reasoning:
            print(data_reasoning, end="", flush=True)
    # Later, stream the final answer
    if hasattr(chunk.choices[0].delta, "content"):
        data_content = chunk.choices[0].delta.content
        if data_content:
            print(data_content, end="", flush=True)
```


---

### Shut down the service

To shutdown your LLM service: 


```python
serve shutdown -y
```


---

## Deploy to production with Anyscale services

For production deployment, use Anyscale services to deploy the Ray Serve app to a dedicated cluster without modifying the code. Anyscale ensures scalability, fault tolerance, and load balancing, keeping the service resilient against node failures, high traffic, and rolling updates.

---

### Launch the service

Anyscale provides out-of-the-box images (`anyscale/ray-llm`), which come pre-loaded with Ray Serve LLM, vLLM, and all required GPU and runtime dependencies. See the [Anyscale base images](https://docs.anyscale.com/reference/base-images) for details on what each image includes.

Build a minimal Dockerfile:
```Dockerfile
FROM anyscale/ray:2.49.0-slim-py312-cu128

# C compiler for Triton’s runtime build step (vLLM V1 engine)
# https://github.com/vllm-project/vllm/issues/2997
RUN sudo apt-get update && \
    sudo apt-get install -y --no-install-recommends build-essential

RUN pip install vllm==0.10.1
```

Create your Anyscale service configuration in a new `service.yaml` file and reference the Dockerfile with `containerfile`:

```yaml
# service.yaml
name: deploy-gpt-oss
containerfile: ./Dockerfile # Build Ray Serve LLM with vllm==0.10.1
compute_config:
  auto_select_worker_config: true 
working_dir: .
cloud:
applications:
  # Point to your app in your Python module
  - import_path: serve_gpt_oss:app
```


Deploy your service:


```python
anyscale service deploy -f service.yaml
```


---

### Send requests 

The `anyscale service deploy` command output shows both the endpoint and authentication token:

```console
(anyscale +3.9s) curl -H "Authorization: Bearer <YOUR-TOKEN>" <YOUR-ENDPOINT>
```

You can also retrieve both from the service page in the Anyscale console. Click **Query** at the top. See [Send requests](#send-requests) for example requests, but make sure to use the correct endpoint and authentication token.  

---

### Access the Serve LLM dashboard

For instructions on enabling LLM-specific logging, see [Enable LLM monitoring](#enable-llm-monitoring). To open the Ray Serve LLM Dashboard from an Anyscale service:

1. In the Anyscale console, go to the **Service** or **Workspace** tab.
1. Navigate to the **Metrics** tab.
1. Click **View in Grafana** and click **Serve LLM Dashboard**.

---

### Shutdown

To shutdown your Anyscale Service:


```python
anyscale service terminate -n deploy-gpt-oss
```


---

## Enable LLM monitoring

The *Serve LLM Dashboard* offers deep visibility into model performance, latency, and system behavior, including:

- Token throughput (tokens/sec).
- Latency metrics: Time To First Token (TTFT), Time Per Output Token (TPOT).
- KV cache utilization.

To enable these metrics, go to your LLM config and set `log_engine_metrics: true`:

```yaml
applications:
- ...
  args:
    llm_configs:
      - ...
        log_engine_metrics: true
```

---

## Improve concurrency

Ray Serve LLM uses [vLLM](https://docs.vllm.ai/en/stable/) as its backend engine, which logs the *maximum concurrency* it can support based on your configuration.

Example log for gpt-oss-20b with 1xL4:
```console
INFO 09-08 17:34:28 [kv_cache_utils.py:1017] Maximum concurrency for 32,768 tokens per request: 5.22x
```

Example log for gpt-oss-120b with 2xL40S:
```console
INFO 09-09 00:32:32 [kv_cache_utils.py:1017] Maximum concurrency for 32,768 tokens per request: 6.18x
```

To improve concurrency for gpt-oss models, see [Deploy a small-sized LLM: Improve concurrency](https://docs.ray.io/en/latest/serve/tutorials/deployment-serve-llm/small-size-llm/README.html#improve-concurrency) for small-sized models such as `gpt-oss-20b`, and [Deploy a medium-sized LLM: Improve concurrency](https://docs.ray.io/en/latest/serve/tutorials/deployment-serve-llm/medium-size-llm/README.html#improve-concurrency) for medium-sized models such as `gpt-oss-120b`.

**Note:** Some example guides recommend using quantization to boost concurrency. `gpt-oss` weights are already 4-bit by default, so further quantization typically isn’t applicable.  

For broader guidance, also see [Choose a GPU for LLM serving](https://docs.anyscale.com/llm/serving/gpu-guidance) and [Optimize performance for Ray Serve LLM](https://docs.anyscale.com/llm/serving/performance-optimization).

---

## Reasoning configuration

You don’t need a custom reasoning parser when deploying `gpt-oss` with Ray Serve LLM, you can access the reasoning content in the model's response directly. You can also control the reasoning effort of the model in the request.

---

### Access reasoning output

The reasoning content is available directly in the `reasoning_content` field of the response:

```python
response = client.chat.completions.create(
    model="my-gpt-oss",
    messages=[
        ...
    ]
)
reasoning_content = response.choices[0].message.reasoning_content
content = response.choices[0].message.content
```

---

### Control reasoning effort

`gpt-oss` supports [three reasoning levels](https://huggingface.co/openai/gpt-oss-20b#reasoning-levels): **low**, **medium**, and **high**. The default level is **medium**.

You can control reasoning with the `reasoning_effort` request parameter:  
```python
response = client.chat.completions.create(
    model="my-gpt-oss",
    messages=[
        {"role": "user", "content": "What are the three main touristic spots to see in Paris?"}
    ],
    reasoning_effort="low" # Or "medium", "high"
)
```

You can also set a level explicitly in the system prompt:  
```python
response = client.chat.completions.create(
    model="my-gpt-oss",
    messages=[
        {"role": "system", "content": "Reasoning: low. You are an AI travel assistant."},
        {"role": "user", "content": "What are the three main touristic spots to see in Paris?"}
    ]
)
```

**Note:** There's no reliable way to completely disable reasoning.

---

## Troubleshooting

### Can't download the vocab file  
```console
openai_harmony.HarmonyError: error downloading or loading vocab file: failed to download or load vocab
```

The `openai_harmony` library needs the *tiktoken* encoding files and tries to fetch them from OpenAI's public host. Common causes include:
- Corporate firewall or proxy blocks `openaipublic.blob.core.windows.net`. You may need to whitelist this domain.
- Intermittent network issues.
- Race conditions when multiple processes try to download to the same cache. This can happen when [deploying multiple models at the same time](https://github.com/openai/harmony/pull/41).

You can also directly download the *tiktoken* encoding files in advance and set the `TIKTOKEN_ENCODINGS_BASE` environment variable:
```bash
mkdir -p tiktoken_encodings
wget -O tiktoken_encodings/o200k_base.tiktoken "https://openaipublic.blob.core.windows.net/encodings/o200k_base.tiktoken"
wget -O tiktoken_encodings/cl100k_base.tiktoken "https://openaipublic.blob.core.windows.net/encodings/cl100k_base.tiktoken"
export TIKTOKEN_ENCODINGS_BASE=${PWD}/tiktoken_encodings
```

### `gpt-oss` architecture not recognized 
```console
Value error, The checkpoint you are trying to load has model type `gpt_oss` but Transformers does not recognize this architecture. This could be because of an issue with the checkpoint, or because your version of Transformers is out of date.
```
Older vLLM and Transformers versions don't register `gpt_oss`, raising an error when vLLM hands off to Transformers. Upgrade **vLLM ≥ 0.10.1** and let your package resolver such as `pip` handle the other dependencies.
```bash
pip install -U "vllm>=0.10.1"
```

---

## Summary

In this tutorial, you learned how to deploy `gpt-oss` models with Ray Serve LLM, from development to production. You learned how to configure Ray Serve LLM, deploy your service on a Ray cluster, send requests, and monitor your service.
