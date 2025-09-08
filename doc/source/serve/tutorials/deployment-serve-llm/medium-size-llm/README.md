---
orphan: true
---

<!--
Do not modify this README. This file is a copy of the notebook and is not used to display the content.
Modify notebook.ipynb instead, then regenerate this file with:
jupyter nbconvert "$notebook.ipynb" --to markdown --output "README.md"
-->

# Deploying a medium-sized LLM

A medium LLM typically runs on a single node with 4-8 GPUs. It offers a balance between performance and efficiency. These models provide stronger accuracy and reasoning than small models while remaining more affordable and resource-friendly than very large ones. This makes them a solid choice for production workloads that need good quality at lower cost. They're also ideal for scaling applications where large models would be too slow or expensive.

This tutorial deploys a medium-sized LLM using Ray Serve LLM. For smaller models, see [Deploying a small-sized LLM](https://docs.ray.io/en/latest/serve/tutorials/deployment-serve-llm/small-size-llm/README.html), and for larger models, see [Deploying a large-sized LLM](https://docs.ray.io/en/latest/serve/tutorials/deployment-serve-llm/large-size-llm/README.html).

---

## Configure Ray Serve LLM

You can deploy a medium-sized LLM on a single node with multiple GPUs. To leverage all available GPUs, set `tensor_parallel_size` to the number of GPUs on the node, which distributes the model’s weights evenly across them.

Ray Serve LLM provides multiple [Python APIs](https://docs.ray.io/en/latest/serve/api/index.html#llm-api) for defining your application. Use [`build_openai_app`](https://docs.ray.io/en/latest/serve/api/doc/ray.serve.llm.build_openai_app.html#ray.serve.llm.build_openai_app) to build a full application from your [`LLMConfig`](https://docs.ray.io/en/latest/serve/api/doc/ray.serve.llm.LLMConfig.html#ray.serve.llm.LLMConfig) object.

Set your Hugging Face token in the config file to access gated models like `Llama-3.1`.


```python
# serve_llama_3_1_70b.py
from ray.serve.llm import LLMConfig, build_openai_app
import os

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="my-llama-3.1-70b",
        # Or unsloth/Meta-Llama-3.1-70B-Instruct for an ungated model
        model_source="meta-llama/Llama-3.1-70B-Instruct",
    ),
    accelerator_type="A100-40G",
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1,
            max_replicas=4,
        )
    ),
    ### If your model is not gated, you can skip `HF_TOKEN`
    # Share your Hugging Face token with the vllm engine so it can access the gated Llama 3.
    # Type `export HF_TOKEN=<YOUR-HUGGINGFACE-TOKEN>` in a terminal
    runtime_env=dict(env_vars={"HF_TOKEN": os.environ.get("HF_TOKEN")}),
    engine_kwargs=dict(
        max_model_len=32768,
        # Split weights among 8 GPUs in the node
        tensor_parallel_size=8,
    ),
)

app = build_openai_app({"llm_configs": [llm_config]})

```

**Note:** Before moving to a production setup, migrate to using a [Serve config file](https://docs.ray.io/en/latest/serve/production-guide/config.html) to make your deployment version-controlled, reproducible, and easier to maintain for CI/CD pipelines. See [Serving LLMs: production guide](https://docs.ray.io/en/latest/serve/llm/serving-llms.html#production-deployment) for an example.

---

## Deploy locally

**Prerequisites**

* Access to GPU compute.
* (Optional) A **Hugging Face token** if using gated models like Meta’s Llama. Store it in `export HF_TOKEN=<YOUR-HUGGINGFACE-TOKEN>`.

**Note: **Depending on the organization, you can usually request access on the model's Hugging Face page. For example, Meta’s Llama model approval can take anywhere from a few hours to several weeks.

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
export HF_TOKEN=<YOUR-HUGGINGFACE-TOKEN>
serve run serve_llama_3_1_70b:app --non-blocking
```

Deployment typically takes a few minutes as the cluster is provisioned, the vLLM server starts, and the model is downloaded. 

---

### Send requests

Your endpoint is available locally at `http://localhost:8000` and you can use a placeholder authentication token for the OpenAI client, for example `"FAKE_KEY"`.

Example curl:


```bash
%%bash
curl -X POST http://localhost:8000/v1/chat/completions \
  -H "Authorization: Bearer FAKE_KEY" \
  -H "Content-Type: application/json" \
  -d '{ "model": "my-llama-3.1-70b", "messages": [{"role": "user", "content": "What is 2 + 2?"}] }'
```

Example Python:


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

Shutdown your LLM service: 


```bash
%%bash
serve shutdown -y
```


---

## Deploy to production with Anyscale services

For production deployment, use Anyscale services to deploy the Ray Serve app to a dedicated cluster without modifying the code. Anyscale ensures scalability, fault tolerance, and load balancing, keeping the service resilient against node failures, high traffic, and rolling updates.

---

### Launch the service

Anyscale provides out-of-the-box images (`anyscale/ray-llm`), which come pre-loaded with Ray Serve LLM, vLLM, and all required GPU/runtime dependencies. This makes it easy to get started without building a custom image.

Create your Anyscale service configuration in a new `service.yaml` file:
```yaml
# service.yaml
name: deploy-llama-3-70b
image_uri: anyscale/ray-llm:2.49.0-py311-cu128 # Anyscale Ray Serve LLM image. Use `containerfile: ./Dockerfile` to use a custom Dockerfile.
compute_config:
  auto_select_worker_config: true 
working_dir: .
cloud:
applications:
  # Point to your app in your Python module
  - import_path: serve_llama_3_1_70b:app
```

Deploy your service. Make sure you forward your Hugging Face token to the command.


```bash
%%bash
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
You can also retrieve both from the service page in the Anyscale console. Click the **Query** button at the top. See [Send requests](#send-requests) for example requests, but make sure to use the correct endpoint and authentication token.  

---

### Access the Serve LLM dashboard

See [Enable LLM monitoring](#enable-llm-monitoring) for instructions on enabling LLM-specific logging. To open the Ray Serve LLM dashboard from an Anyscale service:
1. In the Anyscale console, go to your **Service** or **Workspace**
2. Navigate to the **Metrics** tab
3. Click **View in Grafana** and click **Serve LLM Dashboard**

---

### Shutdown 
 
Shutdown your Anyscale service:


```bash
%%bash
anyscale service terminate -n deploy-llama-3-70b
```


---

## Enable LLM monitoring

The *Serve LLM Dashboard* offers deep visibility into model performance, latency, and system behavior, including:

* Token throughput (tokens/sec).
* Latency metrics: Time To First Token (TTFT), Time Per Output Token (TPOT).
* KV cache utilization.

To enable these metrics, go to your LLM config and set `log_engine_metrics: true`. Ensure vLLM V1 is active with `VLLM_USE_V1: "1"`. 
**Note:** `VLLM_USE_V1: "1"` is the default value with `ray >= 2.48.0` and can be omitted.
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

## Improve concurrency

Ray Serve LLM uses [vLLM](https://docs.vllm.ai/en/latest/) as its backend engine, which logs the *maximum concurrency* it can support based on your configuration.  

Example log:
```console
INFO 08-19 20:57:37 [kv_cache_utils.py:837] Maximum concurrency for 32,768 tokens per request: 13.02x
```

The following are a few ways to improve concurrency depending on your model and hardware:  

**Reduce `max_model_len`**  
Lowering `max_model_len` reduces the memory needed for KV cache.

**Example:** Running Llama-3.1-70&nbsp;B on an A100-40G:
* `max_model_len = 32,768` → concurrency ≈ 13
* `max_model_len = 16,384` → concurrency ≈ 26

**Use Quantized models**  
Quantizing your model (for example, to FP8) reduces the model's memory footprint, freeing up memory for more KV cache and enabling more concurrent requests.

**Use pipeline parallelism**  
If a single node isn't enough to handle your workload, consider distributing the model's layers across multiple nodes with `pipeline_parallel_size > 1`.

**Upgrade to GPUs with more memory**  
Some GPUs provide significantly more room for KV cache and allow for higher concurrency out of the box.

**Scale with more replicas**  
In addition to tuning per-GPU concurrency, you can scale *horizontally* by increasing the number of replicas in your config.  
Each replica runs on its own GPU, so raising the replica count increases the total number of concurrent requests your service can handle, especially under sustained or bursty traffic.
```yaml
deployment_config:
  autoscaling_config:
    min_replicas: 1
    max_replicas: 4
```

*For more details on tuning strategies, hardware guidance, and serving configurations, see [Choose a GPU for LLM serving](https://docs.anyscale.com/llm/serving/gpu-guidance) and [Tune parameters for LLMs on Anyscale services](https://docs.anyscale.com/llm/serving/parameter-tuning).*

---

## Troubleshooting

**Hugging Face auth errors**  
Some models, such as Llama-3.1, are gated and require prior authorization from the organization. See your model’s documentation for instructions on obtaining access.

**Out-of-memory errors**  
Out-of-memory (OOM) errors are one of the most common failure modes when deploying LLMs, especially as model sizes and context length increase.  
See this [Troubleshooting Guide](https://docs.anyscale.com/overview) for common errors and how to fix them.

---

## Summary

In this tutorial, you deployed a medium-sized LLM with Ray Serve LLM, from development to production. You learned how to configure Ray Serve LLM, deploy your service on your Ray cluster, and send requests. You also learned how to monitor your app and troubleshoot common issues.
