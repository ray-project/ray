<!--
Do not modify this README. This file is a copy of the notebook and is not used to display the content.
Modify README.ipynb instead, then regenerate this file with:
jupyter nbconvert "content/small-size-llm/README.ipynb" --to markdown --output "README.md"
Or use this script: bash convert_to_md.sh
-->

# Deploy a small-sized LLM

<div align="left">
<a target="_blank" href="https://console.anyscale.com/template-preview/deployment-serve-llm?file=%252Ffiles%252Fsmall-size-llm"><img src="https://img.shields.io/badge/🚀 Run_on-Anyscale-9hf"></a>&nbsp;
<a href="https://github.com/ray-project/ray/tree/master/doc/source/serve/tutorials/deployment-serve-llm/content/small-size-llm" role="button"><img src="https://img.shields.io/static/v1?label=&amp;message=View%20On%20GitHub&amp;color=586069&amp;logo=github&amp;labelColor=2f363d"></a>&nbsp;
</div>

A small LLM runs on a single node with 1–2 GPUs, making it fast, inexpensive, and simple to use. It’s ideal for prototyping, lightweight applications, latency-critical use cases, cost-sensitive deployments, and environments with limited resources where efficiency matters more than peak accuracy.


For larger models, see [Deploy a medium-sized LLM](https://docs.ray.io/en/latest/serve/tutorials/deployment-serve-llm/content/medium-size-llm/README.html) or [Deploy a large-sized LLM](https://docs.ray.io/en/latest/serve/tutorials/deployment-serve-llm/content/large-size-llm/README.html).

---

## Configure Ray Serve LLM

Ray Serve LLM provides multiple [Python APIs](https://docs.ray.io/en/latest/serve/api/index.html#llm-api) for defining your application. Use [`build_openai_app`](https://docs.ray.io/en/latest/serve/api/doc/ray.serve.llm.build_openai_app.html#ray.serve.llm.build_openai_app) to build a full application from your [`LLMConfig`](https://docs.ray.io/en/latest/serve/api/doc/ray.serve.llm.LLMConfig.html#ray.serve.llm.LLMConfig) object.

Set your Hugging Face token in the config file to access gated models like `Llama-3.1`.


```python
# serve_llama_3_1_8b.py
from ray.serve.llm import LLMConfig, build_openai_app
import os

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="my-llama-3.1-8b",
        # Or unsloth/Meta-Llama-3.1-8B-Instruct for an ungated model
        model_source="meta-llama/Llama-3.1-8B-Instruct",
    ),
    accelerator_type="L4",
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1,
            max_replicas=2,
        )
    ),
    ### If your model isn't gated, you can skip `HF_TOKEN`
    # Share your Hugging Face token with the vllm engine so it can access the gated Llama 3
    # Type `export HF_TOKEN=<YOUR-HUGGINGFACE-TOKEN>` in a terminal
    runtime_env=dict(env_vars={"HF_TOKEN": os.environ.get("HF_TOKEN")}),
    engine_kwargs=dict(max_model_len=8192),
)
app = build_openai_app({"llm_configs": [llm_config]})

```

**Note:** Before moving to a production setup, migrate to a [Serve config file](https://docs.ray.io/en/latest/serve/production-guide/config.html) to make your deployment version-controlled, reproducible, and easier to maintain for CI/CD pipelines. See [Serving LLMs - Quickstart Examples: Production Guide](https://docs.ray.io/en/latest/serve/llm/quick-start.html#production-deployment) for an example.

---

## Deploy locally


**Prerequisites**

* Access to GPU compute.
* (Optional) A **Hugging Face token** if using gated models like Meta's Llama. Store it in `export HF_TOKEN=<YOUR-HUGGINGFACE-TOKEN>`.

**Note:** Depending on the organization, you can usually request access on the model's Hugging Face page. For example, Meta's Llama models approval can take anywhere from a few hours to several weeks.

**Dependencies:**  
```bash
pip install "ray[serve,llm]"
```

---

### Launch

Follow the instructions at [Configure Ray Serve LLM](#configure-ray-serve-llm) to define your app in a Python module `serve_llama_3_1_8b.py`.  

Set your Hugging Face token. In a terminal, run
```bash
export HF_TOKEN=<YOUR-HUGGINGFACE-TOKEN>
```
In a notebook, run:


```python
%env HF_TOKEN=<YOUR-HUGGINGFACE-TOKEN>
```

Deploy your application


```python
!serve run serve_llama_3_1_8b:app --non-blocking
```

Deployment typically takes a few minutes as the cluster is provisioned, the vLLM server starts, and the model is downloaded. 

---

### Send requests

Your endpoint is available locally at `http://localhost:8000`. You can use a placeholder authentication token for the OpenAI client, for example `"FAKE_KEY"`.


```python
#client.py
from urllib.parse import urljoin
from openai import OpenAI

API_KEY = "FAKE_KEY"
BASE_URL = "http://localhost:8000"

client = OpenAI(base_url=urljoin(BASE_URL, "v1"), api_key=API_KEY)

response = client.chat.completions.create(
    model="my-llama-3.1-8b",
    messages=[{"role": "user", "content": "Tell me a joke"}],
    stream=True
)

for chunk in response:
    content = chunk.choices[0].delta.content
    if content:
        print(content, end="", flush=True)
```


---

### Shutdown

Shutdown your LLM service: 


```python
!serve shutdown -y
```


---

## Deploy to production with Anyscale services

For production deployment, use Anyscale services to deploy the Ray Serve app to a dedicated cluster without modifying the code. Anyscale ensures scalability, fault tolerance, and load balancing, keeping the service resilient against node failures, high traffic, and rolling updates. For more details, see [Serve LLMs with Anyscale](https://docs.anyscale.com/llm/serving).

---

### Launch the service

Anyscale provides out-of-the-box images (`anyscale/ray-llm`) which come pre-loaded with Ray Serve LLM, vLLM, and all required GPU/runtime dependencies. This makes it easy to get started without building a custom image.

Create your Anyscale Service configuration in a new `service.yaml` file:

```yaml
# service.yaml
name: deploy-llama-3-8b
image_uri: anyscale/ray-llm:2.54.0-py311-cu128 # Anyscale Ray Serve LLM image. To build an image from a custom Dockerfile, set `containerfile: ./Dockerfile`
compute_config:
  auto_select_worker_config: true 
working_dir: .
cloud:
applications:
  # Point to your app in your Python module
  - import_path: serve_llama_3_1_8b:app
```


Deploy your service. Make sure to forward your Hugging Face token:


```python
!anyscale service deploy -f service.yaml --env HF_TOKEN=<YOUR-HUGGINGFACE-TOKEN>
```

**Custom Dockerfile**  
You can customize the container by building your own Dockerfile. In your Anyscale Service config, reference the Dockerfile with `containerfile` (instead of `image_uri`):

```yaml
# service.yaml
# Replace:
# image_uri: anyscale/ray-llm:2.54.0-py311-cu128

# with:
containerfile: ./Dockerfile
```

See the [Anyscale base images](https://docs.anyscale.com/reference/base-images) for details on what each image includes.

---

### Send requests 

The `anyscale service deploy` command output shows both the endpoint and authentication token:
```console
(anyscale +3.9s) curl -H "Authorization: Bearer <YOUR-TOKEN>" <YOUR-ENDPOINT>
```
You can also retrieve both from the service page in the Anyscale Console. Click the **Query** button at the top. See [Send requests](#send-requests) for example requests, but make sure to use the correct endpoint and authentication token.  

---

### Access the Serve LLM Dashboard

See [Enable LLM monitoring](#enable-llm-monitoring) for instructions on enabling LLM-specific logging. To open the Ray Serve LLM Dashboard from an Anyscale Service:
1. In the Anyscale Console, go to your **Service** or **Workspace**.
2. Navigate to the **Metrics** tab.
3. Click **View in Grafana** and click **Serve LLM Dashboard**.

---

### Shutdown

Shutdown your Anyscale Service:


```python
!anyscale service terminate -n deploy-llama-3-8b
```


---

## Enable LLM monitoring

The *Serve LLM Dashboard* offers deep visibility into model performance, latency, and system behavior, including:

- Token throughput (tokens/sec).
- Latency metrics: Time To First Token (TTFT), Time Per Output Token (TPOT).
- KV cache utilization.

To enable these metrics, go to your LLM config and set `log_engine_metrics: true`. 
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

Example log:
```console
INFO 08-06 20:15:53 [executor_base.py:118] Maximum concurrency for 8192 tokens per request: 3.53x
```

The following are a few ways to improve concurrency depending on your model and hardware:  

**Reduce `max_model_len`**  
Lowering `max_model_len` reduces the memory needed for KV cache.

**Example:** Running *llama-3.1-8&nbsp;B* on an A10G or L4 GPU:
- `max_model_len = 8192` → concurrency ≈ 3.5
- `max_model_len = 4096` → concurrency ≈ 7

**Use quantized models**  
Quantizing your model (for example, to FP8) reduces the model's memory footprint, freeing up memory for more KV cache and enabling more concurrent requests.

**Use tensor parallelism**  
Distribute the model across multiple GPUs with `tensor_parallel_size > 1`.

**Note:** Latency may rise if GPUs don't have high-bandwidth GPU interconnect technology like NVLink.

**Upgrade to GPUs with more memory**  
Some GPUs provide significantly more room for KV cache and allow for higher concurrency out of the box.

**Scale with more replicas**  
In addition to tuning per-replica concurrency, you can scale *horizontally* by increasing the number of replicas in your config.  
Raising the replica count increases the total number of concurrent requests your service can handle, especially under sustained or bursty traffic.
```yaml
deployment_config:
  autoscaling_config:
    min_replicas: 1
    max_replicas: 4
```

*For more details on tuning strategies, hardware guidance, and serving configurations, see [Choose a GPU for LLM serving](https://docs.anyscale.com/llm/serving/gpu-guidance), [Optimize performance for Ray Serve LLM](https://docs.anyscale.com/llm/serving/performance-optimization), and [Tune parameters for LLMs on Anyscale services](https://docs.anyscale.com/llm/serving/parameter-tuning).*

---

## Troubleshooting

**Hugging Face authentication errors**  
Some models, such as Llama-3.1, are gated and require prior authorization from the organization. See your model's documentation for instructions on obtaining access.

**Out-of-memory errors**  
Out-of-memory (OOM) errors are one of the most common failure modes when deploying LLMs, especially as model sizes and context length increase.  
See this [Troubleshooting Guide](https://docs.anyscale.com/llm/serving/troubleshooting) for common errors and how to fix them.

---

## Summary

In this tutorial, you deployed a small-sized LLM with Ray Serve LLM, from development to production. You learned how to configure Ray Serve LLM, deploy your service on your Ray cluster, and send requests. You also learned how to monitor your app and troubleshoot common issues.
