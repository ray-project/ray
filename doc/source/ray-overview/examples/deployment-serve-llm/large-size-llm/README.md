# Deploying a large-size LLM

A large-size LLM typically runs on multiple nodes with multiple GPUs, prioritizing peak quality and capability: stronger reasoning, broader knowledge, longer context windows, more robust generalization. It’s the right choice when state-of-the-art results are required and higher latency, complexity, and cost are acceptable trade-offs.

This tutorial walks you through deploying a large-size LLM like DeepSeek-R1 (685&nbsp;B parameters) using Ray Serve LLM. For smaller model, see [Deploying a small-size LLM](https://docs.ray.io/en/latest/ray-overview/examples/deployment-serve-llm/small-size-llm/README.html) or [Deploying a medium-size LLM](https://docs.ray.io/en/latest/ray-overview/examples/deployment-serve-llm/medium-size-llm/README.html).

---

## Challenges of Large-Scale Deployment

Deploying a 685&nbsp;B-parameter model like DeepSeek-R1 presents significant technical challenges. At this scale, the model can't fit on a single GPU or even a single node. It must be distributed across multiple GPUs and nodes using *tensor parallelism* (splitting tensors within each layer) and *pipeline parallelism* (spreading layers across devices).  

Deploying a model of this scale normally requires manually launching and coordinating multiple nodes, unless you use a managed platform like [Anyscale](https://www.anyscale.com/), which automates cluster scaling and node orchestration. See [Production Deployment with Anyscale Service](#production-deployment-with-anyscale-service) for more details.

---

## Configure Ray Serve LLM

A large-size LLM is typically deployed across multiple nodes with multiple GPUs. To fully utilize the hardware, set `pipeline_parallel_size` to the number of nodes and `tensor_parallel_size` to the number of GPUs per node, which distributes the model’s weights evenly.

Ray Serve LLM provides multiple [Python APIs](https://docs.ray.io/en/latest/serve/api/index.html#llm-api) for defining your application. Use [`build_openai_app`](https://docs.ray.io/en/latest/serve/api/doc/ray.serve.llm.build_openai_app.html#ray.serve.llm.build_openai_app) to build a full application from your [`LLMConfig`](https://docs.ray.io/en/latest/serve/api/doc/ray.serve.llm.LLMConfig.html#ray.serve.llm.LLMConfig) object.


```python
#serve_deepseek_r1.py
from ray.serve.llm import LLMConfig, build_openai_app

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="my-deepseek-r1",
        model_source="deepseek-ai/DeepSeek-R1",
    ),
    accelerator_type="H100",
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1, max_replicas=1,
        )
    ),
    engine_kwargs=dict(
        max_model_len=16384,
        ### Uncomment if your model is gated and need your Huggingface Token to access it
        #hf_token=os.environ.get("HF_TOKEN"),
        # Split weights among 8 GPUs in the node
        tensor_parallel_size=8,
        pipeline_parallel_size=2
    ),
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

**Beware**: this is an expensive deployment.

---

### Launch

Follow the instructions at [Configure Ray Serve LLM](#configure-ray-serve-llm) to define your app in a Python module `serve_deepseek_r1.py`.  

In a terminal, run:  


```bash
%%bash
serve run serve_deepseek_r1:app --non-blocking
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
        "model": "my-deepseek-r1", \
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
    model="my-deepseek-r1",
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

For production, it's recommended to use Anyscale Services to deploy Ray Serve apps on dedicated clusters without code changes. Anyscale provides scalability, fault tolerance, and load balancing, while also automating multi-node setup and autoscaling for large models like DeepSeek-R1.

**Beware**: this is an expensive deployment. At the time of writing, the deployment cost is around \$110 USD per hour in the `us-west-2` AWS region using on-demand instances. Because this node has a high amount of inter-node traffic, and cross-zone traffic is expensive (around \$0.02 per GB), it's recommended to *disable cross-zone autoscaling*. This demo is pre-configured with cross-zone autoscaling disabled for your convenience.

### Prerequisites

The following template runs only on H100 GPUs in your self-hosted Anyscale cloud, as H100s aren't available in Anyscale’s public cloud. This example uses two nodes of type *8xH100-80&nbsp;GB:208CPU-1830&nbsp;GB* on an AWS cloud.

To provision nodes with 1000 GB of disk capacity, see [Changing the default disk size for GCP clusters](https://docs.anyscale.com/configuration/compute/gcp/#changing-the-default-disk-size) for Google Cloud Platform (GCP) or [Changing the default disk size for AWS clusters](https://docs.anyscale.com/configuration/compute/aws/#changing-the-default-disk-size) for Amazon Web Services (AWS). 

---

### Launch

Write your Anyscale Service configuration, in a new `service.yaml` file, write:  
```yaml
#service.yaml
name: deploy-deepseek-r1
image_uri: anyscale/ray-llm:2.48.0-py311-cu128
compute_config:
  auto_select_worker_config: true 
  # Change default disk size to 1000GB
  advanced_instance_config:
    ## AWS ##
    BlockDeviceMappings:
      - Ebs:
        - VolumeSize: 1000
          VolumeType: gp3
          DeleteOnTermination: true
        DeviceName: "/dev/sda1"
    #########
    ## GCP ##
    #instanceProperties:
    #  disks:
    #    - boot: true
    #      auto_delete: true
    #      initialize_params:
    #        - disk_size_gb: 1000
    #########
  
working_dir: .
cloud:
applications:
# Point to your app in your Python module
- import_path: serve_deepseek_r1:app
```

Deploy your Service


```bash
%%bash
anyscale service deploy -f service.yaml
```

> If your model is gated, make sure to pass your HuggingFace Token to the Service with `--env HF_TOKEN=<YOUR_HUGGINGFACE_TOKEN>`

**Custom Dockerfile**

You can use any image from the Anyscale registry, or build your own Dockerfile on top of an Anyscale base image. Create a new `Dockerfile` and start with this minimal setup:
```Dockerfile
FROM anyscale/ray:2.48.0-slim-py312-cu128

# C compiler for Triton’s runtime build step (vLLM V1 engine)
# https://github.com/vllm-project/vllm/issues/2997
RUN sudo apt-get update && \
    sudo apt-get install -y --no-install-recommends build-essential

RUN curl -LsSf https://astral.sh/uv/install.sh | sh

RUN uv pip install --system vllm==0.9.2
# Avoid https://github.com/vllm-project/vllm-ascend/issues/2046 with transformers >= 4.54.0
RUN uv pip install --system transformers==4.53.3
```

In your Anyscale Service config, replace `image_uri` with `containerfile`:
```yaml
#service.yaml
...
## Replace
#image_uri: anyscale/ray-llm:2.48.0-py311-cu128
## With
containerfile: ./Dockerfile # path to your dockerfile
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
anyscale service terminate -n deploy-deepseek-r1
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

Ray Serve LLM uses [vLLM](https://docs.vllm.ai/en/stable/) as its backend engine, which logs the *maximum concurrency* it can support based on your configuration.  

Example log:
```console
INFO 07-30 11:56:04 [kv_cache_utils.py:637] Maximum concurrency for 32,768 tokens per request: 29.06x
```

Here are a few ways to improve concurrency depending on your model and hardware:  

**Reduce `max_model_len`**  
Lowering `max_model_len` reduces the memory needed for KV cache.

> *Example*:  
> Running DeepSeek-R1 on 2 nodes with 8xH100-80&nbsp;GB GPUs each:
> * `max_model_len = 32,768` → concurrency ≈ 29
> * `max_model_len = 16,384` → concurrency ≈ 58

**Use Distilled or Quantized Models**  
Quantizing or distilling your model reduces its memory footprint, freeing up space for more KV cache and enabling more concurrent requests. For example, see [`deepseek-ai/DeepSeek-R1-Distill-Llama-70B`](https://huggingface.co/deepseek-ai/DeepSeek-R1-Distill-Llama-70B) for a distilled version of DeepSeek-R1.


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
