---
orphan: true
---

<!--
Do not modify this README. This file is a copy of the notebook and is not used to display the content.
Modify notebook.ipynb instead, then regenerate this file with:
jupyter nbconvert "$notebook.ipynb" --to markdown --output "README.md"
-->

# Deploy a small-sized LLM

<div align="left">
<a target="_blank" href="https://console.anyscale.com/template-preview/deployment-serve-llm?file=%252Ffiles%252Fsmall-size-llm"><img src="https://img.shields.io/badge/🚀 Run_on-Anyscale-9hf"></a>&nbsp;
<a href="https://github.com/ray-project/ray/tree/master/doc/source/serve/tutorials/deployment-serve-llm/small-size-llm" role="button"><img src="https://img.shields.io/static/v1?label=&amp;message=View%20On%20GitHub&amp;color=586069&amp;logo=github&amp;labelColor=2f363d"></a>&nbsp;
</div>

This tutorial shows you how to deploy and serve a small language model in production with Ray Serve LLM. A small LLM runs on a single node with 1–2 GPUs, making it fast, inexpensive, and simple to use. This tutorial deploys Llama-3.1-8&nbsp;B, a small-sized LLM with 8&nbsp;B parameters. It's ideal for prototyping, lightweight applications, latency-critical use cases, cost-sensitive deployments, and environments with limited resources where efficiency matters more than peak accuracy.

For larger models, see [Deploy a medium-sized LLM](https://docs.ray.io/en/latest/serve/tutorials/deployment-serve-llm/medium-size-llm/README.html) or [Deploy a large-sized LLM](https://docs.ray.io/en/latest/serve/tutorials/deployment-serve-llm/large-size-llm/README.html).

---

## Configure Ray Serve LLM

Ray Serve LLM provides multiple [Python APIs](https://docs.ray.io/en/latest/serve/api/index.html#llm-api) for defining your application. Use [`build_openai_app`](https://docs.ray.io/en/latest/serve/api/doc/ray.serve.llm.build_openai_app.html#ray.serve.llm.build_openai_app) to build a full application from your [`LLMConfig`](https://docs.ray.io/en/latest/serve/api/doc/ray.serve.llm.LLMConfig.html#ray.serve.llm.LLMConfig) object.

Set your Hugging Face token in the config file to access gated models such as `Llama-3.1`.


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

**Note:** Before moving to a production setup, migrate to using a [Serve config file](https://docs.ray.io/en/latest/serve/production-guide/config.html) to make your deployment version-controlled, reproducible, and easier to maintain for CI/CD pipelines. See [Serving LLMs - Quickstart Examples: Production Guide](https://docs.ray.io/en/latest/serve/llm/quick-start.html#production-deployment) for an example.

---

## Deploy locally


**Prerequisites**

* Access to GPU compute.
* (Optional) A **Hugging Face token** if using gated models like Meta’s Llama. Store it in `export HF_TOKEN=<YOUR-HUGGINGFACE-TOKEN>`.

**Note:** Depending on the organization, you can usually request access on the model's Hugging Face page. For example, Meta’s Llama models approval can take anywhere from a few hours to several weeks.

**Dependencies:**  
```bash
pip install "ray[serve,llm]"
```

---

### Launch

Follow the instructions at [Configure Ray Serve LLM](#configure-ray-serve-llm) to define your app in a Python module `serve_llama_3_1_8b.py`.  

In a terminal, run:  


```python
export HF_TOKEN=<YOUR-HUGGINGFACE-TOKEN>
serve run serve_llama_3_1_8b:app --non-blocking
```

Deployment typically takes a few minutes as the cluster is provisioned, the vLLM server starts, and the model is downloaded. 

---

### Send requests

Your endpoint is available locally at `http://localhost:8000`. You can use a placeholder authentication token for the OpenAI client, for example `"FAKE_KEY"`.

Example curl:


```python
curl -X POST http://localhost:8000/v1/chat/completions \
  -H "Authorization: Bearer FAKE_KEY" \
  -H "Content-Type: application/json" \
  -d '{ "model": "my-llama-3.1-8b", "messages": [{"role": "user", "content": "What is 2 + 2?"}] }'
```

Example Python:


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
serve shutdown -y
```


---

## Deploy to production with Anyscale Services

For production deployment, use Anyscale Services to deploy the Ray Serve app to a dedicated cluster without modifying the code. Anyscale ensures scalability, fault tolerance, and load balancing, keeping the service resilient against node failures, high traffic, and rolling updates.

---

### Launch the service

Anyscale provides out-of-the-box images (`anyscale/ray-llm`) which come pre-loaded with Ray Serve LLM, vLLM, and all required GPU/runtime dependencies. This makes it easy to get started without building a custom image.

Create your Anyscale Service configuration in a new `service.yaml` file:

```yaml
# service.yaml
name: deploy-llama-3-8b
image_uri: anyscale/ray-llm:2.49.0-py311-cu128 # Anyscale Ray Serve LLM image. Use `containerfile: ./Dockerfile` to use a custom Dockerfile.
compute_config:
  auto_select_worker_config: true 
working_dir: .
cloud:
applications:
  # Point to your app in your Python module
  - import_path: serve_llama_3_1_8b:app
```


Deploy your service with the following command. Make sure to forward your Hugging Face token:


```python
anyscale service deploy -f service.yaml --env HF_TOKEN=<YOUR-HUGGINGFACE-TOKEN>
```

**Custom Dockerfile**  
You can customize the container by building your own Dockerfile. In your Anyscale Service config, reference the Dockerfile with `containerfile` (instead of `image_uri`):

```yaml
# service.yaml
# Replace:
# image_uri: anyscale/ray-llm:2.49.0-py311-cu128

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

### Access the Serve LLM dashboard

See [Monitor your deployment](#monitor-your-deployment) for instructions on enabling LLM-specific logging. To open the Ray Serve LLM Dashboard from an Anyscale Service:
1. In the Anyscale console, go to your **Service** or **Workspace**.
2. Navigate to the **Metrics** tab.
3. Expand **View in Grafana** and click **Serve LLM Dashboard**.

---

### Shutdown

Shutdown your Anyscale Service:


```python
anyscale service terminate -n deploy-llama-3-8b
```


---

## Monitor your deployment

Ray Serve LLM provides comprehensive monitoring through the Serve LLM Dashboard. This dashboard visualizes key metrics including:

- **Time to First Token (TTFT)**: Latency before the first token is generated.
- **Time Per Output Token (TPOT)**: Average latency per generated token.
- **Token throughput**: Total tokens generated per second.
- **GPU cache utilization**: Percentage of KV cache memory in use.
- **Request latency**: End-to-end request duration.

To enable engine-level metrics, set `log_engine_metrics: true` in your LLM configuration. This is enabled by default starting with Ray 2.51.0.

The following example shows how to enable monitoring:

```python
llm_config = LLMConfig(
    # ... other config ...
    log_engine_metrics=True,  # Enable detailed metrics
)
```

### Access the dashboard

To view metrics in an Anyscale Service or Workspace:

1. Navigate to your **Service** or **Workspace** page.
2. Open the **Metrics** tab.
3. Expand **View in Grafana** and select **Serve LLM Dashboard**.

For a detailed explanation of each metric and how to interpret them for your workload, see [Understand LLM latency and throughput metrics](https://docs.anyscale.com/llm/serving/benchmarking/metrics).

For comprehensive monitoring strategies and best practices, see the [Observability and monitoring guide](https://docs.ray.io/en/latest/serve/llm/user-guides/observability.html).

---

## Improve concurrency

Ray Serve LLM uses [vLLM](https://docs.vllm.ai/en/stable/) as its backend engine, which logs the *maximum concurrency* it can support based on your configuration.

Example log:
```console
INFO 08-06 20:15:53 [executor_base.py:118] Maximum concurrency for 8192 tokens per request: 3.53x
```

You can improve concurrency depending on your model and hardware in several ways:  

**Reduce `max_model_len`**  
Lowering `max_model_len` reduces the memory needed for KV cache.

**Example:** Running *llama-3.1-8&nbsp;B* on an A10G or L4 GPU:
- `max_model_len = 8192` → concurrency ≈ 3.5
- `max_model_len = 4096` → concurrency ≈ 7

**Use Quantized Models**  
Quantizing your model (for example, to FP8) reduces the model's memory footprint, freeing up memory for more KV cache and enabling more concurrent requests.

**Use Tensor Parallelism**  
Distribute the model across multiple GPUs with `tensor_parallel_size > 1`.

**Note:** Latency may rise if GPUs don't have strong GPU interconnect such as NVLink.

**Upgrade to GPUs with more memory**  
Some GPUs provide significantly more room for KV cache and allow for higher concurrency out of the box.

**Scale with more Replicas**  
In addition to tuning per-replica concurrency, you can scale *horizontally* by increasing the number of replicas in your config.  
Raising the replica count increases the total number of concurrent requests your service can handle, especially under sustained or bursty traffic.
```yaml
deployment_config:
  autoscaling_config:
    min_replicas: 1
    max_replicas: 4
```

*For more details on tuning strategies, hardware guidance, and serving configurations, see [Choose a GPU for LLM serving](https://docs.anyscale.com/llm/serving/gpu-guidance) and [Tune parameters for LLMs on Anyscale services](https://docs.anyscale.com/llm/serving/parameter-tuning).*


---

## Troubleshooting

If you encounter issues when deploying your LLM, such as out-of-memory errors, authentication problems, or slow performance, consult the [Troubleshooting Guide](https://docs.anyscale.com/llm/serving/troubleshooting) for solutions to common problems.

---

## Summary

In this tutorial, you deployed a small-sized LLM with Ray Serve LLM, from development to production. You learned how to configure and deploy your service, send requests, monitor performance metrics, and optimize concurrency.

To learn more, take the [LLM Serving Foundations](https://courses.anyscale.com/courses/llm-serving-foundations) course or explore [LLM batch inference](https://docs.anyscale.com/llm/batch-inference) for offline workloads. For larger models, see [Deploy a medium-sized LLM](https://docs.ray.io/en/latest/serve/tutorials/deployment-serve-llm/medium-size-llm/README.html) or [Deploy a large-sized LLM](https://docs.ray.io/en/latest/serve/tutorials/deployment-serve-llm/large-size-llm/README.html).
