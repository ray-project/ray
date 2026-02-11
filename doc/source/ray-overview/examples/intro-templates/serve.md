# Introduction to Ray Serve

This template introduces Ray Serve, a scalable model-serving framework built on Ray. You will learn **what** Ray Serve is, **why** it is a good fit for online ML inference, and **how** to build, deploy, and operate a real model service — starting from a familiar PyTorch classifier and progressively adding features like composition, autoscaling, batching, fault tolerance, and observability.

<div class="alert alert-block alert-info">

**Roadmap:**

1. Why Ray Serve?
2. Key Concepts: Applications, Deployments, Replicas
3. Build Your First Deployment (MNIST Classifier)
4. Integrating with FastAPI
5. Composing Deployments
6. Resource Specification and Fractional GPUs
7. Autoscaling
8. Architecture Overview
9. Request Routing
10. Dynamic Request Batching
11. Fault Tolerance
12. Load Shedding and Backpressure
13. Observability

</div>

## Imports

```python
from typing import Any

import json
import logging
import time

import numpy as np
import requests
import torch
from torchvision import transforms

import ray
from ray import serve
from ray.serve.handle import DeploymentHandle
from ray.serve import metrics
from fastapi import FastAPI
from pydantic import BaseModel
from starlette.requests import Request
from matplotlib import pyplot as plt
```

### Note on Storage

Throughout this tutorial, we use `/mnt/cluster_storage` to represent a shared storage location. In a multi-node cluster, Ray workers on different nodes cannot access the head node's local file system. Use a [shared storage solution](https://docs.anyscale.com/configuration/storage#shared) accessible from every node.

---

## 1. Why Ray Serve?

Consider using Ray Serve when your serving workload has one or more of the following needs:

| **Challenge** | **Ray Serve Solution** |
|---|---|
| **Scalability** — needs to handle variable or high traffic | Autoscaling replicas based on request queue depth; scales across a Ray cluster |
| **Hardware utilization** — GPUs underutilized by one-at-a-time inference | Dynamic request batching and fractional GPU allocation |
| **Service composition** — multiple models or processing stages | Compose deployments; Ray's object store for efficient data sharing |
| **Expensive startup** — large model weights to load | Stateful replicas (Ray actors) keep models in memory across requests |
| **Slow iteration speed** — Kubernetes YAML, container builds | Python-first API; develop locally, deploy distributed with the same code |

#### Key Ray Serve Features

- [Response streaming](https://docs.ray.io/en/latest/serve/tutorials/streaming.html)
- [Dynamic request batching](https://docs.ray.io/en/latest/serve/advanced-guides/dyn-req-batch.html)
- [Multi-node / multi-GPU serving](https://docs.ray.io/en/latest/serve/tutorials/vllm-example.html)
- [Model multiplexing](https://docs.ray.io/en/latest/serve/model-multiplexing.html)
- [Fractional compute resource usage](https://docs.ray.io/en/latest/serve/configure-serve-deployment.html)

---

## 2. Key Concepts: Applications, Deployments, and Replicas

Ray Serve has three core abstractions:

<img src='https://technical-training-assets.s3.us-west-2.amazonaws.com/Ray_Serve/serve_architecture.png' width=700/>

- **Application** — a group of one or more Deployments that are deployed together. Applications can be independently upgraded without affecting other applications on the same cluster.

- **Deployment** — the fundamental building block. Each deployment defines a Python class (or function) that handles requests. Deployments can be independently scaled.

<img src='https://technical-training-assets.s3.us-west-2.amazonaws.com/Ray_Serve/deployment.png' width=600/>

- **Replica** — an instance of a Deployment, implemented as a Ray actor with its own request queue. Each replica can specify its own hardware resources (CPUs, GPUs, memory).

For a deeper dive into these concepts, see the [Ray Serve Key Concepts](https://docs.ray.io/en/latest/serve/key-concepts.html) documentation.

---

## 3. Build Your First Deployment

Let's migrate a standard PyTorch classifier to Ray Serve. We start with a familiar offline `MNISTClassifier` and turn it into an online service.

### 3.1 The Offline Classifier

Here is a standard PyTorch inference class that loads a TorchScript model and classifies images.

```python
class OfflineMNISTClassifier:
    def __init__(self, local_path: str):
        self.model = torch.jit.load(local_path)
        self.model.to("cuda")
        self.model.eval()

    def __call__(self, batch: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
        return self.predict(batch)
    
    def predict(self, batch: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
        images = torch.tensor(batch["image"]).float().to("cuda")

        with torch.no_grad():
            logits = self.model(images).cpu().numpy()

        batch["predicted_label"] = np.argmax(logits, axis=1)
        return batch
```

Download the pre-trained model to shared storage:

```python
!aws s3 cp s3://anyscale-public-materials/ray-ai-libraries/mnist/model/model.pt /mnt/cluster_storage/model.pt
```

We can use this class with Ray Data for batch inference:

```python
ds = ray.data.from_items([{"image": np.random.rand(1, 28, 28)} for _ in range(100)])

ds = ds.map_batches(
    OfflineMNISTClassifier,
    fn_constructor_kwargs={"local_path": "/mnt/cluster_storage/model.pt"},
    concurrency=1,
    num_gpus=1,
    batch_size=10,
)

ds.take_batch(10)["predicted_label"]
```

### 3.2 Migrating to Ray Serve

To turn this into an online service, we make three small changes:

1. Add the `@serve.deployment()` decorator
2. Change `__call__` to accept a Starlette `Request` object
3. Parse the incoming JSON body

```python
@serve.deployment()
class OnlineMNISTClassifier:
    def __init__(self, local_path: str):
        self.model = torch.jit.load(local_path)
        self.model.to("cuda")
        self.model.eval()

    async def __call__(self, request: Request) -> dict[str, Any]:
        batch = json.loads(await request.json())
        return await self.predict(batch)
    
    async def predict(self, batch: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
        images = torch.tensor(batch["image"]).float().to("cuda")

        with torch.no_grad():
            logits = self.model(images).cpu().numpy()

        batch["predicted_label"] = np.argmax(logits, axis=1)
        return batch
```

### 3.3 Deploy and Test

Use `.bind()` to pass constructor arguments and `serve.run()` to deploy:

```python
mnist_deployment = OnlineMNISTClassifier.options(
    num_replicas=1,
    ray_actor_options={"num_gpus": 1},
)

mnist_app = mnist_deployment.bind(local_path="/mnt/cluster_storage/model.pt")
```

> **Note:** `.bind()` is a lazy call — it captures the constructor arguments without creating instances. Replicas are created when `serve.run()` is called.

```python
mnist_handle = serve.run(mnist_app, name="mnist_classifier", blocking=False)
```

Test via HTTP:

```python
images = np.random.rand(2, 1, 28, 28).tolist()
json_request = json.dumps({"image": images})
response = requests.post("http://localhost:8000/", json=json_request)
response.json()["predicted_label"]
```

Test via `DeploymentHandle` (in-process, no HTTP overhead):

```python
batch = {"image": np.random.rand(10, 1, 28, 28)}
response = await mnist_handle.predict.remote(batch)
response["predicted_label"]
```

---

## 4. Integrating with FastAPI

Ray Serve integrates with FastAPI to provide HTTP routing, Pydantic validation, and auto-generated OpenAPI docs. Use `@serve.ingress(fastapi_app)` to designate a FastAPI app as the HTTP entrypoint.

Here we wrap our existing `OnlineMNISTClassifier` pattern into a FastAPI-powered deployment to demonstrate the integration:

```python
fastapi_app = FastAPI()

@serve.deployment
@serve.ingress(fastapi_app)
class MNISTFastAPIService:
    """Same model logic as OnlineMNISTClassifier, but using FastAPI for HTTP routing."""
    def __init__(self, local_path: str):
        self.model = torch.jit.load(local_path)
        self.model.to("cuda")
        self.model.eval()

    @fastapi_app.post("/predict")
    async def predict(self, request: Request):
        batch = json.loads(await request.json())
        images = torch.tensor(batch["image"]).float().to("cuda")
        with torch.no_grad():
            logits = self.model(images).cpu().numpy()
        return {"predicted_label": np.argmax(logits, axis=1).tolist()}

app = MNISTFastAPIService.bind(local_path="/mnt/cluster_storage/model.pt")
serve.run(app, name="mnist_fastapi", blocking=False)
```

```python
images = np.random.rand(2, 1, 28, 28).tolist()
response = requests.post("http://localhost:8000/predict", json=json.dumps({"image": images}))
response.json()["predicted_label"]
```

Visit `http://localhost:8000/docs` for the auto-generated interactive API documentation.

For more details on HTTP handling in Ray Serve, see the [HTTP Guide](https://docs.ray.io/en/latest/serve/http-guide.html).

```python
serve.shutdown()
```

Now that we have a working single-deployment service, let's see how to compose multiple deployments into a pipeline.

---

## 5. Composing Deployments

Ray Serve lets you compose multiple deployments into a single application. This is useful when you need:
- **Independent scaling** — each component scales separately
- **Hardware disaggregation** — CPU preprocessing + GPU inference
- **Reusable components** — share a preprocessor across models

> **Trade-off:** Deployment boundaries introduce serialization + network overhead. If two components use identical resources, consider fusing them into one deployment.

### 5.1 Define a Preprocessor

```python
@serve.deployment
class OnlineMNISTPreprocessor:
    def __init__(self):
        self.transform = transforms.Compose([
            transforms.ToTensor(),
            transforms.Normalize((0.5,), (0.5,))
        ])
        
    async def run(self, batch: dict[str, Any]) -> dict[str, Any]:
        images = batch["image"]
        images = [self.transform(np.array(image, dtype=np.uint8)).cpu().numpy() for image in images]
        return {"image": images}
```

### 5.2 Build a Composed Application

Wire the preprocessor and classifier together via an ingress deployment:

```python
@serve.deployment
class ImageServiceIngress:
    def __init__(self, preprocessor, model):
        self.preprocessor = preprocessor
        self.model = model

    async def __call__(self, request: Request):
        batch = json.loads(await request.json())
        response = await self.preprocessor.run.remote(batch)
        return await self.model.predict.remote(response)
```

```python
image_classifier_app = ImageServiceIngress.bind(
    preprocessor=OnlineMNISTPreprocessor.bind(),
    model=OnlineMNISTClassifier.options(
        num_replicas=1,
        ray_actor_options={"num_gpus": 0.1},
    ).bind(local_path="/mnt/cluster_storage/model.pt"),
)

handle = serve.run(image_classifier_app, name="image_classifier", blocking=False)
```

### 5.3 Test the Composed App

```python
ds = ray.data.read_images("s3://anyscale-public-materials/ray-ai-libraries/mnist/50_per_index/", include_paths=True)
image_batch = ds.take_batch(10)

json_request = json.dumps({"image": image_batch["image"].tolist()})
response = requests.post("http://localhost:8000/", json=json_request)
response.json()["predicted_label"]
```

```python
serve.shutdown()
```

With the composition pattern in hand, let's explore how to fine-tune resource allocation for each deployment.

---

## 6. Resource Specification and Fractional GPUs

Each replica can specify its resource requirements. For small models like our MNIST classifier, you can use **fractional GPUs** to pack multiple replicas on a single GPU:

```python
mnist_app = OnlineMNISTClassifier.options(
    num_replicas=4,
    ray_actor_options={"num_gpus": 0.1},  # 10% of a GPU per replica → up to 10 replicas per GPU
).bind(local_path="/mnt/cluster_storage/model.pt")

mnist_handle = serve.run(mnist_app, name="mnist_classifier", blocking=False)
```

```python
# Test that the fractional GPU deployment still works
images = np.random.rand(2, 1, 28, 28).tolist()
response = requests.post("http://localhost:8000/", json=json.dumps({"image": images}))
response.json()["predicted_label"]
```

For the full list of deployment configuration options, see the [deployment configuration docs](https://docs.ray.io/en/latest/serve/configure-serve-deployment.html).

```python
serve.shutdown()
```

Next, let's see how Ray Serve can automatically scale replicas up and down based on traffic.

---

## 7. Autoscaling

Ray Serve automatically adjusts the number of replicas based on traffic. The autoscaler makes decisions based on the **ongoing requests per replica**.

Scaling decisions cascade through multiple layers: from the Serve controller to the Ray Cluster Autoscaler to the infrastructure (e.g., Kubernetes):

<img src="https://anyscale-public-materials.s3.us-west-2.amazonaws.com/ray-serve/scaling_across_the_stack.png" width="800">

Replicas periodically report their ongoing request count to the autoscaler, which uses these metrics to decide when to add or remove replicas:

<img src="https://anyscale-materials.s3.us-west-2.amazonaws.com/ray-serve-deep-dive/serve-autoscaling-replica-reporting.png" width="800">

### 7.1 Core Configuration

Two key settings drive autoscaling:

- **`target_ongoing_requests`** (default = 2) — the desired average number of active requests per replica. If the actual ratio exceeds 1, Serve scales up.
- **`max_ongoing_requests`** (default = 5) — the upper limit per replica. Set 20–50% higher than `target_ongoing_requests`.

```python
mnist_app = OnlineMNISTClassifier.options(
    ray_actor_options={"num_gpus": 0.1},
    autoscaling_config={
        "target_ongoing_requests": 10,
        "min_replicas": 0,
        "max_replicas": 10,
    },
).bind(local_path="/mnt/cluster_storage/model.pt")
```

### 7.2 Fine-Tuning Autoscaling

You can control how quickly Serve reacts:

| Parameter | Default | Purpose |
|---|---|---|
| `upscale_delay_s` | 30s | Wait before adding replicas |
| `downscale_delay_s` | 600s | Wait before removing replicas |
| `look_back_period_s` | 30s | Window for averaging ongoing requests |
| `upscaling_factor` | 1.0 | Amplify scale-up decisions (>1 for bursty traffic) |
| `downscaling_factor` | 1.0 | Moderate scale-down decisions (<1 for conservative) |

```python
mnist_app = OnlineMNISTClassifier.options(
    ray_actor_options={"num_gpus": 0.1},
    autoscaling_config={
        "target_ongoing_requests": 10,
        "initial_replicas": 0,
        "min_replicas": 0,
        "max_replicas": 10,
        "upscale_delay_s": 5,
        "downscale_delay_s": 60,
        "look_back_period_s": 5,
    },
).bind(local_path="/mnt/cluster_storage/model.pt")

mnist_handle = serve.run(mnist_app, name="mnist_classifier", blocking=False)
```

> **Deprecated API:** `metrics_interval_s` is deprecated. Use the environment variables `RAY_SERVE_REPLICA_AUTOSCALING_METRIC_PUSH_INTERVAL_S` and `RAY_SERVE_HANDLE_AUTOSCALING_METRIC_PUSH_INTERVAL_S` instead.

### 7.3 Autoscaling in Action

With `initial_replicas=0`, no GPU resources are allocated until a request arrives.

> **TODO (Human Review Needed):** Insert a screenshot of the Serve dashboard showing 0 replicas.
> - Source: 5_Intro_Serve
> - Action needed: Run the template and capture a dashboard screenshot

Send requests to trigger scale-up:

```python
batch = {"image": np.random.rand(10, 1, 28, 28)}
[mnist_handle.predict.remote(batch) for _ in range(100)]
```

> **TODO (Human Review Needed):** Insert a screenshot of the Serve dashboard showing replicas scaled up.
> - Source: 5_Intro_Serve
> - Action needed: Run the template and capture a dashboard screenshot after sending requests

```python
serve.shutdown()
```

### 7.4 Tuning Methodology

To find optimal settings for your workload:

1. **Baseline** — deploy with 1 replica, disable autoscaling, and gradually increase QPS until you hit your latency SLA
2. **Set target** — use ~80% of the max capacity as `target_ongoing_requests`
3. **Load test** — use [Locust](https://locust.io/) to simulate steady ramps, traffic spikes, and sustained load
4. **Monitor** — track latency degradation during scale-up transitions, time-to-scale, and request rejections

| Symptom | Likely Cause |
|---|---|
| Frequent request rejections (503) | `max_ongoing_requests` too low |
| Slow scale-up | `upscale_delay_s` too high |
| Replica thrashing (up/down) | Delays too aggressive, traffic at boundary |

For advanced use cases, Ray Serve also supports [custom autoscaling policies](https://docs.ray.io/en/latest/serve/advanced-guides/advanced-autoscaling.html#custom-autoscaling-policies) (alpha API) that go beyond queue-depth — e.g., scaling based on GPU memory or custom business metrics.

With a solid understanding of how to build and scale deployments, let's look under the hood at how Ray Serve processes requests.

---

## 8. Architecture Overview

Ray Serve runs on Ray and consists of three types of actors:

| Actor | Role |
|---|---|
| **Controller** | Global singleton. Manages the control plane, creates/destroys replicas, runs the autoscaler. |
| **Proxy** | Runs a Uvicorn HTTP server (one per head node by default). Accepts incoming HTTP requests and forwards them to replicas. |
| **Replica** | Executes your deployment code. Each replica is a Ray actor with its own request queue. |

<img src="https://docs.ray.io/en/latest/_images/architecture-2.0.svg" width="800">

### Request Flow

1. Request arrives at a `DeploymentHandle` (via proxy or from another deployment)
2. The **Router** in the handle selects an available replica
3. Request is sent to the selected replica (limited by `max_ongoing_requests`)
4. Replica executes user code and returns the response

Queueing happens at **two levels**:

<img src="https://anyscale-materials.s3.us-west-2.amazonaws.com/ray-serve-deep-dive/client_server_routing.png" width="800">

| Location | Config | Purpose |
|---|---|---|
| **Caller side** (Router) | `max_queued_requests` | Limits pending requests waiting for a replica |
| **Receiver side** (Replica) | `max_ongoing_requests` | Limits concurrent requests being processed |

---

## 9. Request Routing

Ray Serve uses the **Power of Two Choices** algorithm by default:

```text
Request arrives
      │
      ▼
┌─────────────────────────────────┐
│  1. Locality filter             │
│     Same Node → Same AZ → All  │
└─────────────┬───────────────────┘
              ▼
┌─────────────────────────────────┐
│  2. Randomly sample 2 replicas  │
└─────────────┬───────────────────┘
              ▼
┌─────────────────────────────────┐
│  3. Pick one with lowest queue  │
└─────────────┬───────────────────┘
              ▼
┌─────────────────────────────────┐
│  4. If all busy → backoff       │
└─────────────────────────────────┘
```

This balances load efficiently without probing every replica. See the ["power of two choices"](https://www.eecs.harvard.edu/~michaelm/postscripts/mythesis.pdf) paper for the theoretical foundation.

<img src="https://anyscale-public-materials.s3.us-west-2.amazonaws.com/ray-serve/power_of_two_choices.png" width="800">

### Model Multiplexing

For multi-model serving, Ray Serve can route requests to replicas that already have the requested model loaded, avoiding redundant model loading. See the [model multiplexing docs](https://docs.ray.io/en/latest/serve/model-multiplexing.html).

### Custom Routers

You can implement custom routing logic by subclassing `RequestRouter`:

```python
from ray.serve._private.request_router import RequestRouter

class MyRouter(RequestRouter):
    async def choose_replicas(self, candidate_replicas, pending_request):
        # Your custom routing logic here
        ...
```

> **Note:** Custom request routing is an alpha API. See the [custom routing docs](https://docs.ray.io/en/latest/serve/advanced-guides/custom-request-router.html).

---

## 10. Dynamic Request Batching

When your model can process multiple inputs efficiently (e.g., GPU inference), batching improves throughput. Ray Serve provides the `@serve.batch` decorator:

```python
@serve.deployment
class BatchMNISTClassifier:
    def __init__(self, local_path: str):
        self.model = torch.jit.load(local_path).to("cuda").eval()

    @serve.batch(max_batch_size=8, batch_wait_timeout_s=0.1)
    async def __call__(self, images_list: list[np.ndarray]) -> list[dict]:
        # images_list is a list of individual request payloads, automatically batched
        stacked = torch.tensor(np.stack(images_list)).float().to("cuda")
        with torch.no_grad():
            logits = self.model(stacked).cpu().numpy()
        predictions = np.argmax(logits, axis=1)
        return [{"predicted_label": int(p)} for p in predictions]
```

Under the hood:
- Requests are buffered in a queue
- Once `max_batch_size` requests arrive (or `batch_wait_timeout_s` elapses), the batch is sent to your method
- Responses are split and returned individually

This is most effective for **vectorized operations on CPUs** and **parallelizable operations on GPUs**.

---

## 11. Fault Tolerance

Ray Serve handles failures at multiple levels:

| Failure | Recovery |
|---|---|
| **Application error** (exception in your code) | Returns HTTP 500 with traceback; replica continues serving |
| **Replica crash** | Controller detects and replaces the replica |
| **Proxy crash** | Controller restarts the proxy |
| **Controller crash** | Ray restarts the Controller |
| **GCS (head node) failure** | Traffic continues; autoscaling paused until GCS recovers |

### Client-Side Retries

When a replica crashes, in-flight requests are lost. Best practice: implement client-side retries with exponential backoff.

```python
# Requires: pip install backoff
import backoff

@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
def make_request_robust(payload):
    response = requests.post("http://localhost:8000/predict", json=payload)
    response.raise_for_status()
    return response
```

> **Best Practice:** If client retries are not feasible, introduce request persistence (e.g., Kafka, SQS) for critical workloads.

For more details, see [End-to-End Fault Tolerance](https://docs.ray.io/en/latest/serve/production-guide/fault-tolerance.html).

---

## 12. Load Shedding and Backpressure

Under heavy load, `DeploymentHandle` queues can grow, causing high tail latency. Ray Serve lets you **shed load** to keep the system stable.

### max_queued_requests

```python
@serve.deployment(
    max_ongoing_requests=2,
    max_queued_requests=2,
)
class ProtectedService:
    def __call__(self, request: Request) -> str:
        time.sleep(2)
        return "Hello!"
```

When the queue limit is reached, new requests immediately receive a `BackPressureError` (HTTP 503).

### Request Timeouts

Set a global timeout in the Serve config:

```yaml
http_options:
  request_timeout_s: 30
```

Requests exceeding the timeout return HTTP 408. Use client-side retries for transient failures.

---

## 13. Observability

### Metrics

Ray Serve exposes metrics at multiple granularity levels:

- **Throughput**: QPS, error QPS (per application, deployment, replica)
- **Latency**: P50, P90, P99 (per application, deployment, replica)
- **Deployment**: replica count, queue size

> **TODO (Human Review Needed):** Insert screenshots of the Anyscale Metrics dashboard showing throughput and latency panels.
> - Source: 04_Observability
> - Action needed: Run the template and capture dashboard screenshots

### Custom Metrics

Define custom metrics using `ray.serve.metrics`:

```python
@serve.deployment(num_replicas=2)
class InstrumentedService:
    def __init__(self):
        self.request_counter = metrics.Counter(
            "request_counter",
            description="Total requests processed.",
            tag_keys=("model",),
        )
        self.request_counter.set_default_tags({"model": "mnist"})

    async def __call__(self, request: Request):
        self.request_counter.inc()
        return "ok"
```

### Logging

Use Python's standard logging module with the `"ray.serve"` logger:

```python
logger = logging.getLogger("ray.serve")
logger.info("Processing request")
```

Logs are written to `/tmp/ray/session_latest/logs/serve/`. Configure logging at the deployment or instance level:

```python
@serve.deployment(logging_config={"log_level": "DEBUG"})
class MyDeployment:
    ...
```

### Health Checks

Implement `check_health()` on your deployment to monitor stateful resources:

```python
@serve.deployment
class DataFetcher:
    def __init__(self, db_url):
        self.db = self._connect(db_url)  # Replace with your actual DB connection

    def _connect(self, url):
        """Stub: replace with your database client."""
        return type("DB", (), {"is_alive": lambda self: True})()

    def __call__(self, request: Request):
        return "ok"

    def check_health(self):
        if not self.db.is_alive():
            raise Exception("Database connection lost")
```

If `check_health()` fails 3 times consecutively, the replica is marked unhealthy and replaced.

For alerts, use [Grafana alerting](https://grafana.com/docs/grafana/latest/alerting/) on Prometheus metrics. For distributed tracing, see the [Anyscale Tracing guide](https://docs.anyscale.com/monitoring/tracing/).

For a comprehensive overview of monitoring and debugging on Anyscale, see the [Anyscale monitoring guide](https://docs.anyscale.com/monitoring) and [custom dashboards and alerting](https://docs.anyscale.com/monitoring/custom-dashboards-and-alerting).

---

## Summary and Next Steps

In this template, you learned how to:

- **Build** a Ray Serve deployment from a standard PyTorch model
- **Integrate** with FastAPI for HTTP routing and validation
- **Compose** multiple deployments into a pipeline
- **Configure** autoscaling, fractional GPUs, and resource allocation
- **Understand** the architecture, request routing, and batching mechanisms
- **Handle** failures with fault tolerance, load shedding, and health checks
- **Monitor** with built-in metrics, custom metrics, logging, and health checks

### Next Steps

1. [Ray Serve documentation](https://docs.ray.io/en/latest/serve/index.html) — full API reference
2. [Production guide](https://docs.ray.io/en/latest/serve/production-guide/index.html) — deploying and managing Serve in production
3. [vLLM on Ray Serve](https://docs.ray.io/en/latest/serve/tutorials/vllm-example.html) — serving LLMs with Ray Serve
4. [Anyscale monitoring guide](https://docs.anyscale.com/monitoring) — dashboards, alerts, and debugging
5. [Configure Serve deployments](https://docs.ray.io/en/latest/serve/configure-serve-deployment.html) — full configuration options

---

## Cleanup

```python
serve.shutdown()
!rm -rf /mnt/cluster_storage/model.pt
```
