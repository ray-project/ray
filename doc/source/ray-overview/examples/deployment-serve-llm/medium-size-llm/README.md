# Deploying a medium-size LLM

A medium-size LLM typically runs on a single node with 4—8 GPUs and offers a balance between performance and efficiency. It provides stronger accuracy and reasoning than small models while remaining more affordable and resource-friendly than very large ones. This makes it a solid choice for production workloads that need good quality at lower cost, or for scaling applications where large models would be too slow or expensive.

This tutorial walks you through deploying a medium-size LLM using Ray Serve LLM. For smaller model, see [Deploying a small-size LLM](https://docs.ray.io/en/latest/ray-overview/examples/deployment-serve-llm/small-size-llm/README.html), and for larger models, see [Deploying a large-size LLM](https://docs.ray.io/en/latest/ray-overview/examples/deployment-serve-llm/large-size-llm/README.html).

---

## Configure Ray Serve LLM

Make sure to set your Hugging Face token in the config file to access gated models like `Llama-3.1`.

A medium-sized LLM can typically be deployed on a single node with multiple GPUs. To leverage all available GPUs, set `tensor_parallel_size` to the number of GPUs on the node, which distributes the model’s weights evenly across them.

Ray Serve LLM provides multiple [Python APIs](https://docs.ray.io/en/latest/serve/api/index.html#llm-api) for defining your application. Use [`build_openai_app`](https://docs.ray.io/en/latest/serve/api/doc/ray.serve.llm.build_openai_app.html#ray.serve.llm.build_openai_app) to build a full application from your [`LLMConfig`](https://docs.ray.io/en/latest/serve/api/doc/ray.serve.llm.LLMConfig.html#ray.serve.llm.LLMConfig) object.


```python
#serve_llama_3_1_70b.py
from ray.serve.llm import LLMConfig, build_openai_app
import os

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="my-llama-3.1-70b",
        # Or Qwen/Qwen2.5-72B-Instruct for an ungated model
        model_source="meta-llama/Llama-3.1-70B-Instruct",
    ),
    accelerator_type="A100-40G",
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1, max_replicas=4,
        )
    ),
    ### If your model is not gated, you can skip `hf_token`
    # Share your Hugging Face Token to the vllm engine so it can access the gated Llama 3
    # Type `export HF_TOKEN=<YOUR-HUGGINGFACE-TOKEN>` in a terminal
    engine_kwargs=dict(
        max_model_len=32768,
        # Split weights among 8 GPUs in the node
        tensor_parallel_size=8
    )
)

app = build_openai_app({"llm_configs": [llm_config]})
```

> Before moving to a production setup, it's recommended to switch to a [Serve config file](https://docs.ray.io/en/latest/serve/production-guide/config.html). This makes your deployment version-controlled, reproducible, and easier to maintain for CI/CD pipelines for example. See [Serving LLMs: Production Guide](https://docs.ray.io/en/latest/serve/llm/serving-llms.html#production-deployment) for an example.

---

## Local End-to-End Deployment

**Prerequisites**

* Access to GPU compute.
* (Optional) A **Hugging Face token** if using gated models like Meta’s Llama. Store it in `export HF_TOKEN=<YOUR-HUGGINGFACE-TOKEN>`

> Depending on the organization, you can usually request access on the model's Hugging Face page. For example, Meta’s Llama models approval can take anywhere from a few hours to several weeks.

**Dependencies:**  
```bash
pip install "ray[serve,llm]"
```

---

### Launch

Follow the instructions at [Configure Ray Serve LLM](#configure-ray-serve-llm) to define your app in a Python module `serve_llama_3_1_70b.py`.  

In a terminal, run:  


```bash
%%bash
serve run serve_llama_3_1_70b:app --non-blocking
```

Deployment typically takes a few minutes as the cluster is provisioned, the vLLM server starts, and the model is downloaded. 

---

### Sending Requests

Your endpoint is available locally at `http://localhost:8000` and you can use a placeholder authentication token for the OpenAI client, for example `"FAKE_KEY"`

Example Curl


```bash
%%bash
curl -X POST http://localhost:8000/v1/chat/completions \
  -H "Authorization: Bearer FAKE_KEY" \
  -H "Content-Type: application/json" \
  -d '{ \
        "model": "my-llama-3.1-70b", \
        "messages": [{"role": "user", "content": "What is 2 + 2?"}] \
      }'
```

Example Python


```python
#client.py
from urllib.parse import urljoin
from openai import OpenAI

api_key = "FAKE_KEY"
base_url = "http://localhost:8000"

client = OpenAI(base_url=urljoin(base_url, "v1"), api_key=api_key)

response = client.chat.completions.create(
    model="my-llama-3.1-70b",
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


```bash
%%bash
serve shutdown -y
```


---

## Production Deployment with Anyscale Service

For production deployment, use Anyscale services to deploy the Ray Serve app to a dedicated cluster without modifying the code. Anyscale ensures scalability, fault tolerance, and load balancing, keeping the service resilient against node failures, high traffic, and rolling updates.

---

### Launch

Anyscale provides out-of-the-box images (`anyscale/ray-llm`) which comes preloaded with Ray Serve LLM, vLLM, and all required GPU/runtime dependencies. This makes it easy to get started without building a custom image.

Write your Anyscale Service configuration in a new `service.yaml` file:
```yaml
#service.yaml
name: deploy-llama-3-70b
image_uri: anyscale/ray-llm:2.49.0-py311-cu128
compute_config:
  auto_select_worker_config: true 
working_dir: .
cloud:
applications:
# Point to your app in your Python module
- import_path: serve_llama_3_1_70b:app
```

Deploy your Service, make sure you set your HuggingFace Token first, `export HF_TOKEN=<YOUR-HUGGINGFACE-TOKEN>`


```bash
%%bash
anyscale service deploy -f service.yaml --env HF_TOKEN=$HF_TOKEN
```

**Custom Dockerfile**

The recommended image for most cases is `anyscale/ray-llm`. However, if you prefer a minimal image for more control, here’s an example `Dockerfile` based on the lighter `anyscale/ray` base image that installs only what you need to run this example.
```Dockerfile
FROM anyscale/ray:2.49.0-slim-py312-cu128

# C compiler for Triton’s runtime build step (vLLM V1 engine)
# https://github.com/vllm-project/vllm/issues/2997
RUN sudo apt-get update && \
    sudo apt-get install -y --no-install-recommends build-essential

RUN pip install vllm==0.10.0
```

In your Anyscale Service config, point to this Dockerfile with `containerfile` instead of `image_uri`:

```yaml
# service.yaml
...
## Recommended (default):
# image_uri: anyscale/ray-llm:2.49.0-py311-cu128
## Minimal example (custom):
containerfile: ./Dockerfile  # path to your Dockerfile
...
```

---

### Sending Requests 

Both the endpoint and authentication token are shown in the output of the `anyscale service deploy` command:
```console
(anyscale +3.9s) curl -H "Authorization: Bearer <YOUR-TOKEN>" <YOUR-ENDPOINT>
```
You can also retrieve both from the service page in the Anyscale Console. Just click the **Query** button at the top. See [Sending Requests](#sending-requests) for example requests, but make sure to put the correct endpoint and authentication token.  

---

### Serve LLM Dashboard

See [Enable LLM Monitoring](#enable-llm-monitoring) for instructions on enabling LLM-specific logging. To open the Ray Serve LLM Dashboard from an Anyscale Service:
1. In the Anyscale console, go to your **Service** or **Workspace**
2. Navigate to the **Metrics** tab
3. Expand **View in Grafana** and click **Serve LLM Dashboard**

---

### Shutdown 
 
Shutdown your Anyscale Service


```bash
%%bash
anyscale service terminate -n deploy-llama-3-70b
```


---

## Enable LLM Monitoring

The *Serve LLM Dashboard* offers deep visibility into model performance, latency, and system behavior, including:

* Token throughput (tokens/sec)
* Latency metrics: Time To First Token (TTFT), Time Per Output Token (TPOT)
* KV cache utilization

To enable these metrics, go to your LLM config and set `log_engine_metrics: true`. Ensure vLLM V1 is active with `VLLM_USE_V1: "1"`. 
> `VLLM_USE_V1: "1"` is the default value with `ray >= 2.48.0` and can be omitted.
```yaml
applications:
- ...
  args:
    llm_configs:
      - ...
        runtime_env:
          env_vars:
            VLLM_USE_V1: "1"
        ...
        log_engine_metrics: true
```

---

## Improving Concurrency

Ray Serve LLM uses [vLLM](https://docs.vllm.ai/en/latest/) as its backend engine, which logs the *maximum concurrency* it can support based on your configuration.  

Example log:
```console
INFO 08-19 20:57:37 [kv_cache_utils.py:837] Maximum concurrency for 32,768 tokens per request: 13.02x
```

Here are a few ways to improve concurrency depending on your model and hardware:  

**Reduce `max_model_len`**  
Lowering `max_model_len` reduces the memory needed for KV cache.

> *Example*:  
> Running llama-3.1-70&nbsp;B On an A100-40G:
> * `max_model_len = 32,768` → concurrency ≈ 13
> * `max_model_len = 16,384` → concurrency ≈ 26

**Use Quantized Models**  
Quantizing your model (for example, to FP8) reduces the model's memory footprint, freeing up memory for more KV cache and enabling more concurrent requests.

**Use Pipeline Parallelism**  
If a single node isn't enough to handle your workload, consider distributing the model's layers across multiple nodes with `pipeline_parallel_size > 1`.

**Upgrade to GPUs with more memory**  
Some GPUs provide significantly more room for KV cache and allow for higher concurrency out of the box.

**Scale with more Replicas**  
In addition to tuning per-GPU concurrency, you can scale *horizontally* by increasing the number of replicas in your config.  
Each replica runs on its own GPU, so raising the replica count increases the total number of concurrent requests your service can handle, especially under sustained or bursty traffic.
```yaml
deployment_config:
  autoscaling_config:
    min_replicas: 1
    max_replicas: 4
```

*For more details on tuning strategies, hardware guidance, and serving configurations, see the [GPU Selection Guide for LLM Serving](https://docs.anyscale.com/overview) and [Tuning vLLM and Ray Serve Parameters for LLM Deployment](https://docs.anyscale.com/overview).*

---

## Troubleshooting

**HuggingFace Auth Errors**  
Some models, such as Llama-3.1, are gated and require prior authorization from the organization. See your model’s documentation for instructions on obtaining access.

**Out-Of-Memory Errors**  
Out‑of‑memory (OOM) errors are one of the most common failure modes when deploying LLMs, especially as model sizes, and context length increase.  
See this [Troubleshooting Guide](https://docs.anyscale.com/overview) for common errors and how to fix them.

---

## Summary

In this tutorial, you deployed a medium-size LLM with Ray Serve LLM, from development to production. You learned how to configure Ray Serve LLM, deploy your service on your Ray Cluster, and how to send requests. you also learned how to monitor your app and common troubleshooting issues.
