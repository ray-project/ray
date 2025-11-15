---
orphan: true
---

<!--
Do not modify this README. This file is a copy of the notebook and is not used to display the content.
Modify notebook.ipynb instead, then regenerate this file with:
jupyter nbconvert "$notebook.ipynb" --to markdown --output "README.md"
-->

# Deploy a large-sized LLM

<div align="left">
<a target="_blank" href="https://console.anyscale.com/template-preview/deployment-serve-llm?file=%252Ffiles%252Flarge-size-llm"><img src="https://img.shields.io/badge/🚀 Run_on-Anyscale-9hf"></a>&nbsp;
<a href="https://github.com/ray-proj    ect/ray/tree/master/doc/source/serve/tutorials/deployment-serve-llm/large-size-llm" role="button"><img src="https://img.shields.io/static/v1?label=&amp;message=View%20On%20GitHub&amp;color=586069&amp;logo=github&amp;labelColor=2f363d"></a>&nbsp;
</div>

This tutorial shows you how to deploy and serve a large language model in production with Ray Serve LLM. A large LLM typically runs on multiple nodes with multiple GPUs, prioritizing peak quality and capability: stronger reasoning, broader knowledge, longer context windows, more robust generalization. This tutorial deploys DeepSeek-R1, a large-sized LLM with 685&nbsp;B parameters. When higher latency, complexity, and cost are acceptable trade-offs because you require state-of-the-art results.

For smaller models, see [Deploy a small-sized LLM](https://docs.ray.io/en/latest/serve/tutorials/deployment-serve-llm/small-size-llm/README.html) or [Deploy a medium-sized LLM](https://docs.ray.io/en/latest/serve/tutorials/deployment-serve-llm/medium-size-llm/README.html).

---

## Challenges of large-scale deployments

Deploying a 685&nbsp;B-parameter model like DeepSeek-R1 presents significant technical challenges. At this scale, the model can't fit on a single GPU or even a single node. You must distribute it across multiple GPUs and nodes using *tensor parallelism* (splitting tensors within each layer) and *pipeline parallelism* (spreading layers across devices).  

Deploying a model of this scale normally requires you to manually launch and coordinate multiple nodes, unless you use a managed platform like [Anyscale](https://www.anyscale.com/), which automates cluster scaling and node orchestration. See [Deploy to production with Anyscale Services](#deploy-to-production-with-anyscale-services) for more details.

---

## Configure Ray Serve LLM

A large-sized LLM is typically deployed across multiple nodes with multiple GPUs. To fully utilize the hardware, set `pipeline_parallel_size` to the number of nodes and `tensor_parallel_size` to the number of GPUs per node, which distributes the model’s weights evenly.

Ray Serve LLM provides multiple [Python APIs](https://docs.ray.io/en/latest/serve/api/index.html#llm-api) for defining your application. Use [`build_openai_app`](https://docs.ray.io/en/latest/serve/api/doc/ray.serve.llm.build_openai_app.html#ray.serve.llm.build_openai_app) to build a full application from your [`LLMConfig`](https://docs.ray.io/en/latest/serve/api/doc/ray.serve.llm.LLMConfig.html#ray.serve.llm.LLMConfig) object.

**Optional:** Because Deepseek-R1 is a reasoning model, this tutorial uses vLLM’s built-in reasoning parser to correctly separate its reasoning content from the final response. See [Deploying a reasoning LLM: Parse reasoning outputs](https://docs.ray.io/en/latest/serve/tutorials/deployment-serve-llm/reasoning-llm/README.html#parse-reasoning-outputs).


```python
# serve_deepseek_r1.py
from ray.serve.llm import LLMConfig, build_openai_app

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="my-deepseek-r1",
        model_source="deepseek-ai/DeepSeek-R1",
    ),
    accelerator_type="H100",
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1,
            max_replicas=1,
        )
    ),
    ### Uncomment if your model is gated and needs your Hugging Face token to access it.
    # runtime_env=dict(env_vars={"HF_TOKEN": os.environ.get("HF_TOKEN")}),
    engine_kwargs=dict(
        max_model_len=16384,
        # Split weights among 8 GPUs in the node
        tensor_parallel_size=8,
        pipeline_parallel_size=2,
        reasoning_parser="deepseek_r1",  # Optional: separate reasoning content from the final answer
    ),
)

app = build_openai_app({"llm_configs": [llm_config]})

```

**Note:** Before moving to a production setup, migrate to a [Serve config file](https://docs.ray.io/en/latest/serve/production-guide/config.html) to make your deployment version-controlled, reproducible, and easier to maintain for CI/CD pipelines. See [Serving LLMs - Quickstart Examples: Production Guide](https://docs.ray.io/en/latest/serve/llm/quick-start.html#production-deployment) for an example.

---

## Deploy locally

**Prerequisites**

* Access to GPU compute.
* (Optional) A **Hugging Face token** if using gated models. Store it in `export HF_TOKEN=<YOUR-HUGGINGFACE-TOKEN>`.

**Note:** Depending on the organization, you can usually request access on the model's Hugging Face page. For example, Meta’s Llama models approval can take anywhere from a few hours to several weeks.

**Dependencies:**  
```bash
pip install "ray[serve,llm]"
```

**Beware**: this is an expensive deployment.

---

### Launch

Follow the instructions at [Configure Ray Serve LLM](#configure-ray-serve-llm) to define your app in a Python module `serve_deepseek_r1.py`.  

In a terminal, run:  


```python
serve run serve_deepseek_r1:app --non-blocking
```

Deployment typically takes a few minutes as the cluster is provisioned, the vLLM server starts, and the model is downloaded. 

---

### Send requests

Your endpoint is available locally at `http://localhost:8000` and you can use a placeholder authentication token for the OpenAI client, for example `"FAKE_KEY"`.

Example curl:


```python
curl -X POST http://localhost:8000/v1/chat/completions \
  -H "Authorization: Bearer FAKE_KEY" \
  -H "Content-Type: application/json" \
  -d '{ "model": "my-deepseek-r1", "messages": [{"role": "user", "content": "What is 2 + 2?"}] }'
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
    model="my-deepseek-r1",
    messages=[{"role": "user", "content": "Tell me a joke"}],
    stream=True,
)

# Stream and print JSON
for chunk in response:
    # Stream reasoning content first
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

### Shutdown

Shutdown your LLM service: 


```python
serve shutdown -y
```


---

## Deploy to production with Anyscale services

For production deployment, use Anyscale services to deploy the Ray Serve app to a dedicated cluster without modifying the code. Anyscale provides scalability, fault tolerance, and load balancing, keeping the service resilient against node failures, high traffic, and rolling updates, while also automating multi-node setup and autoscaling for large models like DeepSeek-R1.

**Beware**: this is an expensive deployment. At the time of writing, the deployment cost is around \$110 USD per hour in the `us-west-2` AWS region using on-demand instances. Because this node has a high amount of inter-node traffic, and cross-zone traffic is expensive (around \$0.02 per GB), it's recommended to *disable cross-zone autoscaling*. This demo is pre-configured with cross-zone autoscaling disabled for your convenience.

### Prerequisites

The following template runs only on H100 GPUs in your self-hosted Anyscale cloud, as H100s aren't available in Anyscale’s public cloud. This example uses two nodes of type *8xH100-80&nbsp;GB:208CPU-1830&nbsp;GB* on an AWS cloud.

To provision nodes with 1000 GB of disk capacity, see [Changing the default disk size for GCP clusters](https://docs.anyscale.com/configuration/compute/gcp#disk-size) for Google Cloud Platform (GCP) or [Changing the default disk size for AWS clusters](https://docs.anyscale.com/configuration/compute/aws#disk-size) for Amazon Web Services (AWS). 

---

### Launch the service

Anyscale provides out-of-the-box images (`anyscale/ray-llm`), which come pre-loaded with Ray Serve LLM, vLLM, and all required GPU/runtime dependencies. This makes it easy to get started without building a custom image.

Create your Anyscale service configuration in a new `service.yaml` file:
```yaml
#service.yaml
name: deploy-deepseek-r1
image_uri: anyscale/ray-llm:2.49.0-py311-cu128 # Anyscale Ray Serve LLM image. Use `containerfile: ./Dockerfile` to use a custom Dockerfile.
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

Deploy your service


```python
anyscale service deploy -f service.yaml
```

**Note:** If your model is gated, make sure to pass your Hugging Face token to the service with `--env HF_TOKEN=<YOUR_HUGGINGFACE_TOKEN>`

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

See [Monitor your deployment](#monitor-your-deployment) for instructions on enabling LLM-specific logging. To open the Ray Serve LLM dashboard from an Anyscale service:
1. In the Anyscale console, go to your **Service** or **Workspace**
2. Navigate to the **Metrics** tab
3. Click **View in Grafana** and click **Serve LLM Dashboard**

---

### Shutdown 
 
Shutdown your Anyscale service:


```python
anyscale service terminate -n deploy-deepseek-r1
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
INFO 07-30 11:56:04 [kv_cache_utils.py:637] Maximum concurrency for 32,768 tokens per request: 29.06x
```

The following are a few ways to improve concurrency depending on your model and hardware:  

**Reduce `max_model_len`**  
Lowering `max_model_len` reduces the memory needed for KV cache.

**Example**: Running DeepSeek-R1 on 2 nodes with 8xH100-80&nbsp;GB GPUs each:
* `max_model_len = 32,768` → concurrency ≈ 29
* `max_model_len = 16,384` → concurrency ≈ 58

**Use distilled or quantized models**  
Quantizing or distilling your model reduces its memory footprint, freeing up space for more KV cache and enabling more concurrent requests. For example, see [`deepseek-ai/DeepSeek-R1-Distill-Llama-70B`](https://huggingface.co/deepseek-ai/DeepSeek-R1-Distill-Llama-70B) for a distilled version of DeepSeek-R1.


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

*For more details on tuning strategies, hardware guidance, and serving configurations, see [Choose a GPU for LLM serving](https://docs.anyscale.com/llm/serving/gpu-guidance) and [Tune parameters for LLMs on Anyscale services](https://docs.anyscale.com/llm/serving/parameter-tuning).*

---

## Troubleshooting

If you encounter issues when deploying your LLM, such as out-of-memory errors, authentication problems, or slow performance, consult the [Troubleshooting Guide](https://docs.anyscale.com/llm/serving/troubleshooting) for solutions to common problems.

---

## Summary

In this tutorial, you deployed a large-sized LLM with Ray Serve LLM, from development to production. You learned how to configure and deploy your service, send requests, monitor performance metrics, and optimize concurrency.

To learn more, take the [LLM Serving Foundations](https://courses.anyscale.com/courses/llm-serving-foundations) course or explore [LLM batch inference](https://docs.anyscale.com/llm/batch-inference) for offline workloads. For smaller models, see [Deploy a small-sized LLM](https://docs.ray.io/en/latest/serve/tutorials/deployment-serve-llm/small-size-llm/README.html) or [Deploy a medium-sized LLM](https://docs.ray.io/en/latest/serve/tutorials/deployment-serve-llm/medium-size-llm/README.html).
