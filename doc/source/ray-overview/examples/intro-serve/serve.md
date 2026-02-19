# Introduction to Ray Serve

This template introduces Ray Serve, a scalable model-serving framework built on Ray. You will learn **what** Ray Serve is, **why** it is a good fit for online ML inference, and **how** to build, deploy, and operate a real model service — starting from a familiar PyTorch classifier and progressively adding features like composition, autoscaling, batching, fault tolerance, and observability.

**Part 1: Core**

1. Why Ray Serve?
2. Build Your First Deployment (MNIST Classifier)
3. Integrating with FastAPI
4. Composing Deployments
5. Resource Specification and Fractional GPUs
6. Autoscaling
7. Observability

**Part 2: Advanced topics**

8. Dynamic Request Batching
9. Model Multiplexing
10. Asynchronous Inference
11. Custom Request Routing
12. Custom Autoscaling

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
| **Model composition** — multiple models or processing stages | Compose heterogeneous deployments with independent scaling; Efficient data transfer between deployments through the Ray object store |
| **Slow iteration speed** — Kubernetes YAML, container builds | Python-first API; develop locally, deploy distributed with the same code |

---

## 2. Build Your First Deployment

Let's migrate a standard PyTorch classifier to Ray Serve. We start with a familiar offline `MNISTClassifier` and turn it into an online service.

### 2.1 The Offline Classifier

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

### 2.2 Migrating to Ray Serve

To turn this into an online service, we make three changes:

1. Add the `@serve.deployment()` decorator — this turns the class into a **Deployment**, Ray Serve's fundamental unit that can be independently scaled and configured
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
        batch = await request.json()
        return await self.predict(batch)

    async def predict(self, batch: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
        images = torch.from_numpy(np.stack(batch["image"])).float().to("cuda")

        with torch.no_grad():
            logits = self.model(images).cpu().numpy()

        batch["predicted_label"] = np.argmax(logits, axis=1)
        return batch
```

### 2.3 Deploy and Test

Use `.bind()` to pass constructor arguments and `serve.run()` to deploy. Setting `num_replicas=1` creates a single **Replica** — a Ray actor that holds your model in memory and processes requests.

```python
mnist_deployment = OnlineMNISTClassifier.options(
    num_replicas=1,
    ray_actor_options={"num_gpus": 1},
)

mnist_app = mnist_deployment.bind(local_path="/mnt/cluster_storage/model.pt")
```

Deployment configuration — replicas, resources, autoscaling, and more — can be specified in three ways:

- **`@serve.deployment` decorator** — set defaults at class definition time; useful when the config is stable across environments
- **`.options()`** — override at bind time; useful for environment-specific tuning without changing source code
- **Anyscale Service YAML** — declarative configuration for production deployments on Anyscale; supports per-environment overrides without code changes. See the [Anyscale Services docs](https://docs.anyscale.com/services/tutorial) for details.

Using the decorator:

```python
@serve.deployment(num_replicas=1, ray_actor_options={"num_gpus": 1})
class OnlineMNISTClassifier:
    ...

mnist_app = OnlineMNISTClassifier.bind(local_path="/mnt/cluster_storage/model.pt")
```

Using `.options()` (overrides decorator defaults):

```python
mnist_app = OnlineMNISTClassifier.options(
    num_replicas=2,
    ray_actor_options={"num_gpus": 1},
).bind(local_path="/mnt/cluster_storage/model.pt")
```

See the [full list of deployment configuration options](https://docs.ray.io/en/latest/serve/configure-serve-deployment.html).

`serve.run()` creates an **Application** — a group of deployments deployed together — and starts the Serve system:

```python
mnist_handle = serve.run(mnist_app, name="mnist_classifier", blocking=False)
```

#### Under the hood

When `serve.run()` returns, Ray Serve has started three types of actors:

| Actor | Role |
|---|---|
| **Controller** | Global singleton. Manages the control plane, creates/destroys replicas, runs the autoscaler. |
| **Proxy** | Runs a Uvicorn HTTP server (one per head node by default). Accepts incoming HTTP requests and forwards them to replicas. |
| **Replica** | Executes your deployment code. Each replica is a Ray actor with its own request queue. |

<img src="https://docs.ray.io/en/latest/_images/architecture-2.0.svg" width="800">

These actors are self-healing: if a replica crashes, the Controller detects and replaces it; if the Proxy crashes, the Controller restarts it; if the Controller itself crashes, Ray restarts it. Application exceptions (bugs in your code) return HTTP 500 but don't take down the replica. For critical workloads, implement client-side retries with exponential backoff. See [End-to-End Fault Tolerance](https://docs.ray.io/en/latest/serve/production-guide/fault-tolerance.html) for details.

#### Test via HTTP

When you send a request to `localhost:8000`, the **Proxy** receives it, the **Router** selects a replica, and the replica executes your `__call__` method:

```python
images = np.random.rand(2, 1, 28, 28).tolist()
response = requests.post("http://localhost:8000/", json={"image": images})
response.json()["predicted_label"]
```

#### Test via DeploymentHandle

You can also call deployments in-process without HTTP overhead:

```python
batch = {"image": np.random.rand(10, 1, 28, 28)}
response = await mnist_handle.predict.remote(batch)
response["predicted_label"]
```

```python
serve.shutdown()
```

---

## 3. Integrating with FastAPI

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
        batch = await request.json()
        images = torch.from_numpy(np.stack(batch["image"])).float().to("cuda")
        with torch.no_grad():
            logits = self.model(images).cpu().numpy()
        return {"predicted_label": np.argmax(logits, axis=1).tolist()}

app = MNISTFastAPIService.options(
        num_replicas=1,
        ray_actor_options={"num_gpus": 0.1},
    ).bind(local_path="/mnt/cluster_storage/model.pt")
serve.run(app, name="mnist_fastapi", blocking=False)
```

```python
images = np.random.rand(2, 1, 28, 28).tolist()
response = requests.post("http://localhost:8000/predict", json={"image": images})
response.json()["predicted_label"]
```

For more details on HTTP handling in Ray Serve, see the [HTTP Guide](https://docs.ray.io/en/latest/serve/http-guide.html).

```python
serve.shutdown()
```

Now that we have a working single-deployment service, let's see how to compose multiple deployments into a pipeline.

---

## 4. Composing Deployments

Ray Serve lets you compose multiple deployments into a single application. This is useful when you need:
- **Independent scaling** — each component scales separately
- **Hardware disaggregation** — CPU preprocessing + GPU inference
- **Reusable components** — share a preprocessor across models

### 4.1 Define a Preprocessor

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

### 4.2 Build a Composed Application

Wire the preprocessor and classifier together via an ingress deployment:

```python
@serve.deployment
class ImageServiceIngress:
    def __init__(self, preprocessor, model):
        self.preprocessor = preprocessor
        self.model = model

    async def __call__(self, request: Request):
        batch = await request.json()
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

### 4.3 Test the Composed App

```python
ds = ray.data.read_images("s3://anyscale-public-materials/ray-ai-libraries/mnist/50_per_index/", include_paths=True)
image_batch = ds.take_batch(10)

response = requests.post("http://localhost:8000/", json={"image": image_batch["image"].tolist()})
response.json()["predicted_label"]
```

```python
serve.shutdown()
```

With the composition pattern in hand, let's explore how to fine-tune resource allocation for each deployment.

---

## 5. Resource Specification and Fractional GPUs

Each replica can specify its resource requirements. For small models like our MNIST classifier, you can use **fractional GPUs** to pack multiple replicas on a single GPU:

```python
mnist_app = OnlineMNISTClassifier.options(
    num_replicas=4,
    ray_actor_options={"num_gpus": 0.1},  # 10% of a GPU per replica → up to 10 replicas per GPU
).bind(local_path="/mnt/cluster_storage/model.pt")

mnist_handle = serve.run(mnist_app, name="mnist_classifier", blocking=False)
```

#### Request routing
With multiple replicas, Serve uses the **Power of Two Choices** algorithm by default: randomly sample 2 replicas, pick the one with the shorter queue. For workloads requiring cache affinity, latency-aware selection, or priority queues, see [Section 11: Custom Request Routing](#11-custom-request-routing).

---
Test the fractional GPU deployment
```python
images = np.random.rand(2, 1, 28, 28).tolist()
response = requests.post("http://localhost:8000/", json={"image": images})
response.json()["predicted_label"]
```

```python
serve.shutdown()
```

Next, let's see how Ray Serve can automatically scale replicas up and down based on traffic.

---

## 6. Autoscaling

Ray Serve automatically adjusts the number of replicas based on traffic. The key settings are:

- **`target_ongoing_requests`** — the desired average number of active requests per replica. The autoscaler adds replicas when the actual ratio exceeds this target.
- **`max_ongoing_requests`** — the upper limit per replica. Set 20-50% higher than `target_ongoing_requests`. While `max_ongoing_requests` limits concurrency per replica, `max_queued_requests` limits how many requests wait in the caller's queue. When reached, new requests immediately receive HTTP 503.
- **`upscale_delay_s`** / **`downscale_delay_s`** — how long to wait before adding or removing replicas.
- **`look_back_period_s`** — the time window for averaging ongoing requests when making scaling decisions.

### Autoscaling in action

With `initial_replicas=0` and `min_replicas=0`, no GPU resources are allocated until a request arrives:

```python
mnist_app = OnlineMNISTClassifier.options(
    ray_actor_options={"num_cpus": 0.5, "num_gpus": 0.1},
    autoscaling_config={
        "target_ongoing_requests": 10,
        "initial_replicas": 0,
        "min_replicas": 0,
        "max_replicas": 8,
        "upscale_delay_s": 5,
        "downscale_delay_s": 60,
        "look_back_period_s": 5,
    },
).bind(local_path="/mnt/cluster_storage/model.pt")

mnist_handle = serve.run(mnist_app, name="mnist_classifier", blocking=False)
```

Send requests to trigger scale-up:

```python
batch = {"image": np.random.rand(10, 1, 28, 28)}
[mnist_handle.predict.remote(batch) for _ in range(200)]
```

<img src="https://anyscale-public-materials.s3.us-west-2.amazonaws.com/intro-ai-libraries/serve-auto-scaling.png" width="800">


```python
serve.shutdown()
```

For workloads where request count doesn't correlate with actual load — predictable traffic patterns, resource-constrained stages, latency SLAs, or multi-deployment pipelines — see [Section 12: Custom Autoscaling](#12-custom-autoscaling).

---

## 7. Observability

### Metrics

Ray Serve exposes metrics at multiple granularity levels through the Serve dashboard and Grafana:

- **Throughput metrics** — QPS and error QPS, available per application, per deployment, and per replica

<img src="https://anyscale-public-materials.s3.us-west-2.amazonaws.com/intro-ai-libraries/serve-throughput-metrics.png" width="800">

- **Latency metrics** — P50, P90, P99 latencies at the same granularity levels

<img src="https://anyscale-public-materials.s3.us-west-2.amazonaws.com/intro-ai-libraries/serve-latency-metrics.png" width="800">

- **Deployment metrics** — replica count and queue size per deployment

<img src="https://anyscale-public-materials.s3.us-west-2.amazonaws.com/intro-ai-libraries/serve-replica-metrics.png" width="400">

<img src="https://anyscale-public-materials.s3.us-west-2.amazonaws.com/intro-ai-libraries/serve-queuesize-metrics.png" width="400">

Access these through the Ray Dashboard by navigating to **Ray Dashboard > Serve > VIEW IN GRAFANA**.

### Custom metrics

Define custom metrics using `ray.serve.metrics`:

```python
@serve.deployment(num_replicas=2)
class InstrumentedService:
    def __init__(self):
        self.request_counter = metrics.Counter(
            "my_request_counter",
            description="Total requests processed.",
            tag_keys=("model",),
        )
        self.request_counter.set_default_tags({"model": "mnist"})

    async def __call__(self, request: Request):
        self.request_counter.inc()
        return "ok"
```

To create custom dashboards for monitoring your custom metrics, see [Custom dashboards and alerting](https://docs.anyscale.com/monitoring/custom-dashboards-and-alerting).

Here is how the custom metric looks like in the Anyscale dashboard.

<img src="https://anyscale-public-materials.s3.us-west-2.amazonaws.com/intro-ai-libraries/serve-custom-request-counter.png" width="400">

### Tracing

For end-to-end request tracing across composed deployments, use the Anyscale Tracing integration. A single request's trace displays the hierarchical structure of how it flows through your deployment graph:

```text
1. proxy_http_request (Root) - Duration: 245ms
   └── 2. proxy_route_to_replica (APIGateway) - Duration: 240ms
       └── 3. replica_handle_request (APIGateway) - Duration: 235ms
           └── 4. proxy_route_to_replica (UserService) - Duration: 180ms
               └── 5. replica_handle_request (UserService) - Duration: 175ms
```

For details, see the [Anyscale Tracing guide](https://docs.anyscale.com/monitoring/tracing/).

### Alerts

Ray integrates with Prometheus and Grafana for an enhanced observability experience. Grafana alerting lets you set up alerts based on Prometheus metrics — for example, alerting when P90 latency exceeds your SLA or error QPS spikes. Grafana supports multiple notification channels including Slack and PagerDuty.

For a comprehensive overview of monitoring and debugging on Anyscale, see the [Anyscale monitoring guide](https://docs.anyscale.com/monitoring) and [custom dashboards and alerting](https://docs.anyscale.com/monitoring/custom-dashboards-and-alerting).

---

# Part 2: Advanced Topics

The following sections cover additional serving patterns, operational features, and production concerns.

---

## 8. Dynamic Request Batching

When your model can process multiple inputs efficiently (such as GPU inference), batching improves throughput. Ray Serve provides the `@serve.batch` decorator:

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

## 9. Model Multiplexing

When serving many models with the same shape but different weights (such as per-customer fine-tuned models), model multiplexing lets a shared pool of replicas efficiently serve all of them. The router inspects the `serve_multiplexed_model_id` request header and routes each request to a replica that already has that model loaded, avoiding redundant loading. Each replica caches up to `max_num_models_per_replica` models and evicts the least recently used one when full.

<img src="https://anyscale-public-materials.s3.us-west-2.amazonaws.com/intro-ai-libraries/model_multiplexing_architecture.png" width="800">

For the full API walkthrough — including code examples, client headers, and `DeploymentHandle` options — see the [Model Multiplexing docs](https://docs.ray.io/en/latest/serve/model-multiplexing.html).

---

## 10. Asynchronous Inference

Synchronous APIs block until processing completes, which is problematic for long-running tasks such as video processing or document analysis. Asynchronous inference decouples request submission from result retrieval — clients submit a task, receive a task ID immediately, and poll for the result later.

<img src="https://anyscale-materials.s3.us-west-2.amazonaws.com/ray-serve-deep-dive/async_inference_architecture.png" width="900">

The architecture consists of an HTTP ingress that enqueues tasks into a broker (such as Redis or RabbitMQ), a `@task_consumer` deployment that pulls and processes tasks, and a backend that stores results and status. This provides natural backpressure, built-in retries, and dead letter queues for failed tasks.

For the full walkthrough — including configuration, code examples, and monitoring — see the [Asynchronous Inference docs](https://docs.ray.io/en/latest/serve/asynchronous-inference.html).

---

## 11. Custom Request Routing

For routing decisions that require replica-specific state — which KV-cache prefix is loaded, per-user session affinity, or request priority — subclass `RequestRouter` and implement `choose_replicas()`, which returns a ranked list of candidate groups. The base class handles queue-length probing, exponential backoff, and dead-replica removal.

```python
from ray.serve._private.request_router import RequestRouter, FIFOMixin, LocalityMixin

class MyRouter(FIFOMixin, LocalityMixin, RequestRouter):
    async def choose_replicas(self, candidate_replicas, pending_request):
        candidates = self.apply_locality_routing(pending_request)
        # sort by a custom stat exposed via record_routing_stats()
        return [sorted(candidates, key=lambda r: r.routing_stats.get("load", 0))]
```

Optional mixins compose common behaviors: `FIFOMixin` for FIFO ordering, `LocalityMixin` for same-node → same-AZ preference, and `MultiplexMixin` for model-affinity routing. Replicas report custom statistics to the router by implementing `record_routing_stats() -> dict[str, float]`, polled periodically by the Controller.

For the full walkthrough — uniform random router, throughput-aware router, and the complete `RunningReplica` API — see the [Custom Request Routing docs](https://docs.ray.io/en/latest/serve/advanced-guides/custom-request-router.html).

---

## 12. Custom Autoscaling

Custom policies let you encode any scaling logic in Python — pre-scale by time of day, respond to CPU/memory metrics reported by replicas, target a P90 latency SLA, or coordinate replica counts across a multi-deployment pipeline.

```python
from ray.serve.config import AutoscalingConfig, AutoscalingPolicy

def scheduled_policy(ctx: AutoscalingContext) -> tuple[int, dict]:
    hour = datetime.now(ZoneInfo("America/Los_Angeles")).hour
    desired = 8 if 9 <= hour < 17 else (4 if 7 <= hour < 20 else 1)
    return max(ctx.capacity_adjusted_min_replicas,
               min(ctx.capacity_adjusted_max_replicas, desired)), {}

@serve.deployment(autoscaling_config=AutoscalingConfig(
    min_replicas=1, max_replicas=12,
    policy=AutoscalingPolicy(policy_function=scheduled_policy),
))
class MyDeployment: ...
```

The Controller calls your policy at each tick with an `AutoscalingContext` containing the current target replica count, per-replica metrics from `record_autoscaling_stats()`, and state returned from the previous tick. Always use `ctx.target_num_replicas` as the baseline — not `ctx.current_num_replicas` — since it reflects pending decisions that haven't materialized yet. Application-level policies receive contexts for all deployments at once and return joint scaling decisions, enabling proportional scaling across pipeline stages.

For the full walkthrough — schedule-based, CPU/memory metrics, Prometheus latency SLA, and the external scaler REST API — see the [Custom Autoscaling docs](https://docs.ray.io/en/latest/serve/advanced-guides/advanced-autoscaling.html#custom-autoscaling-policies).

---

## Summary and Next Steps

In this template, you learned how to:

- **Build** a Ray Serve deployment from a standard PyTorch model
- **Integrate** with FastAPI for HTTP routing and validation
- **Compose** multiple deployments into a pipeline
- **Configure** autoscaling, fractional GPUs, and resource allocation
- **Monitor** with built-in metrics, custom metrics, tracing, and alerts
- **Understand** batching, model multiplexing, and async inference patterns
- **Customize** autoscaling with custom policies (schedule, resource, latency SLA, pipeline coordination)
- **Extend** request routing with custom replica selection logic (cache affinity, priority, latency-aware)

### Next Steps

1. [Ray Serve documentation](https://docs.ray.io/en/latest/serve/index.html) — full API reference
2. [Production guide](https://docs.ray.io/en/latest/serve/production-guide/index.html) — deploying and managing Serve in production
3. [Anyscale monitoring guide](https://docs.anyscale.com/monitoring) — dashboards, alerts, and debugging
4. [Configure Serve deployments](https://docs.ray.io/en/latest/serve/configure-serve-deployment.html) — full configuration options

---
