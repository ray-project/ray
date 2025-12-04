---
orphan: true
---

<!--
Do not modify this README. This file is a copy of the notebook and is not used to display the content.
Modify notebook.ipynb instead, then regenerate this file with:
jupyter nbconvert "content/notebook.ipynb" --to markdown --output "README.md"
Or use this script: bash convert_to_md.sh
-->

# Model multiplexing with forecasting models

<div align="left">
<a target="_blank" href="https://console.anyscale.com/template-preview/model_multiplexing_forecast"><img src="https://img.shields.io/badge/🚀 Run_on-Anyscale-9hf"></a>&nbsp;
<a href="https://github.com/ray-project/ray/tree/master/doc/source/serve/tutorials/model_multiplexing_forecast/content" role="button"><img src="https://img.shields.io/static/v1?label=&message=View%20On%20GitHub&color=586069&logo=github&labelColor=2f363d"></a>&nbsp;
</div>

This tutorial shows you how to efficiently serve multiple forecasting models using Ray Serve's model multiplexing pattern. Model multiplexing lets you serve dozens or thousands of models from a shared pool of replicas, optimizing cost and resources.

## Why model multiplexing for forecasting?

Forecasting applications often require separate models per customer, team, or region. Standing up separate deployments for each model is expensive and wasteful. Model multiplexing solves this by:

- **Sharing resources**: Multiple models use the same replica pool.
- **Lazy loading**: Models load on-demand when first requested.
- **Automatic caching**: Frequently used models stay in memory using Least Recently Used (LRU) policy.
- **Intelligent routing**: Requests route to replicas that already have the model loaded.

See [Model Multiplexing](https://docs.ray.io/en/latest/serve/model-multiplexing.html) for the core concepts and API reference.

Use model multiplexing when you have:
- Many models that can fit in memory.
- Sparse access patterns (not all models queried constantly).
- Similar input/output formats across models.
- Limited infrastructure budget.

## Write a multiplexed deployment

Assume you have multiple forecasting models stored in cloud storage with the following structure:
```
/customer_123/model.pkl
/customer_456/model.pkl
/customer_789/model.pkl
...
```

Define a multiplexed deployment:


```python
# serve_forecast_multiplex.py
import asyncio
import numpy as np
import pickle
from ray import serve
from starlette.requests import Request


# Simple forecasting model
class ForecastModel:
    """A customer-specific forecasting model."""
    
    def __init__(self, customer_id: str):
        self.customer_id = customer_id
        # Each customer has different model parameters
        np.random.seed(hash(customer_id) % 1000)
        self.trend = np.random.uniform(-1, 3)
        self.base_level = np.random.uniform(90, 110)
    
    def predict(self, sequence_data: list) -> list:
        """Generate a 7-day forecast."""
        last_value = sequence_data[-1] if sequence_data else self.base_level
        forecast = []
        for i in range(7):
            # Simple forecast: last value + trend
            value = last_value + self.trend * (i + 1)
            forecast.append(round(value, 2))
        return forecast


@serve.deployment
class ForecastingService:
    def __init__(self):
        # In production, this is your cloud storage path or model registry
        self.model_storage_path = "/customer-models"
    
    @serve.multiplexed(max_num_models_per_replica=4)
    async def get_model(self, customer_id: str):
        """Load a customer's forecasting model.
        
        In production, this function downloads from cloud storage or loads from a database.
        For this example, the code mocks the I/O with asyncio.sleep().
        """
        # Simulate downloading model from remote storage
        await asyncio.sleep(0.1)  # Mock network I/O delay
        
        # In production:
        # model_bytes = await download_from_storage(f"{self.model_storage_path}/{customer_id}/model.pkl")
        # return pickle.loads(model_bytes)
        
        # For this example, create a mock model
        return ForecastModel(customer_id)
    
    async def __call__(self, request: Request):
        """Generate forecast for a customer."""
        # Get the serve_multiplexed_model_id from the request header
        customer_id = serve.get_multiplexed_model_id()
        
        # Load the model (cached if already loaded)
        model = await self.get_model(customer_id)
        
        # Get input data
        data = await request.json()
        sequence_data = data.get("sequence_data", [])
        
        # Generate forecast
        forecast = model.predict(sequence_data)
        
        return {"customer_id": customer_id, "forecast": forecast}


app = ForecastingService.bind()

```

The `@serve.multiplexed` decorator enables automatic caching with LRU eviction. The `max_num_models_per_replica` parameter controls how many models to cache per replica. When the cache fills, Ray Serve evicts the least recently used model.

See [Model Multiplexing](https://docs.ray.io/en/latest/serve/model-multiplexing.html) for details on how the caching and routing work.

## Deploy locally

Test your multiplexed deployment on your local machine before moving to production.

### Launch

In a terminal, run:


```python
!serve run serve_forecast_multiplex:app --non-blocking
```

**Note:** When running in a notebook, the `--non-blocking` flag returns control immediately so you can continue executing cells. Without it, `serve run` blocks the notebook. In a terminal, you can omit this flag to stream logs to the console.

Ray Serve logs the endpoint of your application once the service is deployed:
```console
INFO 2025-12-04 01:46:12,733 serve 5085 -- Application 'default' is ready at http://0.0.0.0:8000/
```

### Send requests

To send a request to a specific customer's model, include the `serve_multiplexed_model_id` header:


```python
# client.py
import requests

# time series data
sequence_data = [100, 102, 98, 105, 110, 108, 112, 115, 118, 120]

# Send request with customer_id in header
response = requests.post(
    "http://localhost:8000",
    headers={"serve_multiplexed_model_id": "customer_123"},
    json={"sequence_data": sequence_data}
)

print(response.json())

```

Output:
```json
{
  "customer_id": "customer_123",
  "forecast": [121.45, 122.90, 124.35, 125.80, 127.25, 128.70, 130.15]
}
```

The first request to a model triggers loading that you can track in the logs:

```
INFO 2025-12-04 00:50:18,097 default_ForecastingService -- Loading model 'customer_123'.
INFO 2025-12-04 00:50:18,197 default_ForecastingService -- Successfully loaded model 'customer_123' in 100.1ms.
```

Subsequent requests for the same model use the cache instead, unless the model has been evicted, which you can also track in the logs:
```
INFO 2025-12-04 01:59:08,141 default_ForecastingService -- Unloading model 'customer_def'.
INFO 2025-12-04 01:59:08,142 default_ForecastingService -- Successfully unloaded model 'customer_abc' in 0.2ms.
```

### Test multiple customers

Send requests for different customers to see multiplexing in action:



```python
# client_multiple_requests.py
import random
import requests

customer_ids = ["customer_123", "customer_456", "customer_789", "customer_abc", "customer_def", "customer_hij"]

# Create a random list of 100 requests from those 3 customers
random_requests = [random.choice(customer_ids) for _ in range(100)]

# Send all 100 requests
for i, customer_id in enumerate(random_requests):
    # Generate random "live" data for each request
    live_sequence_data = [random.uniform(90, 110) for _ in range(10)]
    
    response = requests.post(
        "http://localhost:8000",
        headers={"serve_multiplexed_model_id": customer_id},
        json={"sequence_data": live_sequence_data}
    )
    forecast = response.json()["forecast"]
    print(f"Request {i+1} - {customer_id}: {forecast[:3]}...")

print(f"\nSent {len(random_requests)} requests total")

```

What happens:
1. First request for each customer triggers model loading (~100&nbsp;ms).
2. Subsequent requests use the cached model (<5&nbsp;ms).
3. When cache fills (>10 models per replica), least recently used models evict.

You can also send requests using the deployment handle:

```python
handle = serve.get_deployment_handle("ForecastingService", "default")
result = await handle.options(multiplexed_model_id="customer_123").remote(request)
```

### Shutdown

Shutdown your service:


```python
!serve shutdown -y
```

## Deploy to production with Anyscale Services

For production deployment, use Anyscale Services to deploy the application to a dedicated cluster.

Create a `service.yaml` file:

```yaml
# service.yaml
name: my-forecast-multiplexing-service
image_uri: anyscale/ray:2.52.1-slim-py312
compute_config:
  auto_select_worker_config: true
working_dir: .
applications:
  - import_path: serve_forecast_multiplex:app
```

### Launch

Deploy your Anyscale Service:
```bash
anyscale service deploy -f service.yaml
```

The output shows your endpoint URL and authentication token.
```console
(anyscale +6.6s) Query the service once it's running using the following curl command (add the path you want to query):
(anyscale +6.6s) curl -H "Authorization: Bearer 4I9gMWIdpsfmNufE9-DLPXBRXvvwMjIfMfqtEzZq0Qc" https://forecast-multiplexing-service-jgz99.cld-kvedzwag2qa8i5bj.s.anyscaleuserdata.com/
```

You can also retrieve them from your console. Go to your Anyscale Service page, then click the **Query** button at the top.

### Send requests

Use the endpoint and token from the deployment output:



```python
# client_anyscale_service.py
import requests

ENDPOINT = "<YOUR-ENDPOINT>"  # From the deployment output
TOKEN = "<YOUR-TOKEN>"  # From the deployment output

response = requests.post(
    ENDPOINT,
    headers={
        "Authorization": f"Bearer {TOKEN}",
        "serve_multiplexed_model_id": "customer_123"
    },
    json={"historical_values": [100, 102, 98, 105, 110]}
)

print(response.json())

```

### Shutdown

Terminate your Anyscale Service:
```bash
anyscale service terminate -n my-forecast-multiplexing-service
```

## Monitor your deployment

Ray Serve exposes metrics for model multiplexing:

| Metric | Description |
|--------|-------------|
| `ray_serve_num_multiplexed_models` | Number of models loaded per replica |
| `ray_serve_multiplexed_model_load_latency_ms` | Model load time |
| `ray_serve_multiplexed_models_load_counter_total` | Total models loaded |
| `ray_serve_multiplexed_models_unload_counter_total` | Total models unloaded |

See [Monitoring and Debugging](https://docs.ray.io/en/latest/serve/monitoring.html) for more details on monitoring a Serve application.

### Access the dashboard on Anyscale

To view metrics in an Anyscale Service or Workspace:

1. From your console, navigate to your Anyscale Service or Workspace page.
2. Go to the **Metrics** tab, then **Serve Deployment Dashboard**.

From there, you can also open Grafana by clicking **View tab in Grafana**.

## Performance tips

Optimize your multiplexed deployment for better cache hit and lower latency.

### Use co-routines for blocking operations during model loading

Use async operations for network or I/O operations:

```python

@serve.multiplexed(max_num_models_per_replica=10)
async def get_model(self, customer_id: str):
    ...
    model = await long_download_model()
    await long_io_operation()
    ...
    return model
```

### Tune cache size

Set `max_num_models_per_replica` based on your memory and access patterns:

```python
# For small models (10MB each) on 4GB nodes
@serve.multiplexed(max_num_models_per_replica=100)

# For larger models (100MB each)
@serve.multiplexed(max_num_models_per_replica=20)
```

### Scale replicas

Increase the number of replicas if you want more models to be warm:

```python
@serve.deployment(num_replicas=5)  # 5 × 10 = 50 warm models
```

### Pre-warm models

Consider pre-loading important models in the cache during initialization to avoid costly cold-starts:

```python
async def _prewarm(self):
    ...
    for customer_id in ["customer_vip_1", "customer_vip_2"]:
        await self.get_model(customer_id)
```

### Calculate cache hit rate

Monitor the ratio of model loads to model requests by looking at metrics such as `ray_serve_multiplexed_models_load_counter_total` or `ray_serve_multiplexed_get_model_requests_counter_total`.

A high cache hit rate (>90%) means most requests use cached models. A low rate suggests:
- Too many unique models for cache size
- Need to increase `max_num_models_per_replica`
- Consider adding more replicas

### Implement resource cleanup

If your models hold resources (GPU memory, database connections), implement `__del__` in the object returned by your loading model function (the one decorated by `serve.multiplexed()`):

```python

@serve.deployment
class ForecastingService:
    ...
    @serve.multiplexed()
    async def load_model(self):
        ...
        return ForecastModel()

class ForecastModel:
    def __init__(self, model_id: str):
        self.model_id = model_id
        self.db_connection = connect_to_db()
        self.get_lock()
    
    def __del__(self):
        """Clean up resources when model is evicted."""
        if hasattr(self, 'db_connection'):
            self.db_connection.close()
        self.release_lock()
        self.clean_gpu_memory()
        ...
```

Ray Serve automatically calls `__del__` when evicting models from the cache.

## Troubleshooting

Common issues you might encounter when using model multiplexing.

### Missing model ID in request

```
ValueError: The model ID cannot be empty.
```

Make sure you pass `serve_multiplexed_model_id` in the header of your request, or in the handle options:
```python
# Forward `serve_multiplexed_model_id` in the request header
response = requests.post(
    "http://localhost:8000",
    headers={"serve_multiplexed_model_id": "customer_123"},
    json={"sequence_data": sequence_data}
)
# Or
# Forward `serve_multiplexed_model_id` in the handle options
await self.forecaster.options(
    multiplexed_model_id=request.model_id
).remote(request)
```

### TypeError: object can't be used in 'await' expression

```
TypeError: object <...> can't be used in 'await' expression
```

Make sure you define your model loading function, decorated with `@serve.multiplexed()`, as an async function with `async def`:

```python
@serve.deployment
class ForecastingService:
    ...
    @serve.multiplexed(max_num_models_per_replica=10)
    async def my_loading_model_function(self, my_model_id: str):
        ...
```

### Slow first requests

When you send the first request for a model, the system loads the model into the cache. Subsequent requests are fast until the system evicts the model from the cache according to the LRU policy.

If you wish to avoid cold-starts, consider [awaiting blocking IO/network operations](#use-coroutines-for-blocking-operations-during-model-loading), [pre-warming important models](#pre-warm-models), [increasing the cache size](#tune-cache-size), or [add more replicas](#scale-replicas) to your deployment.

### Cache thrashing

If you observe high latency variance and your models are constantly loading and unloading, you might have too many models for the cache size.

For example:
- 100 different models requested per minute
- Cache size: 10 models per replica
- Result: Constant eviction and reloading

See [Performance tips](#performance-tips) for guidance on how to optimize your deployment.

### Out of memory errors

If the system loads your models simultaneously, they might exceed available memory, especially when some models require GPU memory. Reduce `max_num_models_per_replica` to limit the size of your cache. 

## Summary

This tutorial shows you how to use model multiplexing to serve multiple forecasting models from shared replicas, configure deployments with the `@serve.multiplexed` decorator and cache tuning, deploy both locally and in production, route requests with model IDs, monitor performance, optimize your deployment, and troubleshoot common issues.
