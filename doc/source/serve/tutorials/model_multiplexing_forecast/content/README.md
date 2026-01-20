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

## Configure a multiplexed deployment

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


class ForecastModel:
    """A customer-specific forecasting model.
    
    Note: If your models hold resources (GPU memory, database connections),
    implement __del__ to clean up when Ray Serve evicts the model from cache.
    """
    
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
    
    def __del__(self):
        """Clean up resources when model is evicted from cache."""
        # Example: close database connections, release GPU memory, etc.
        pass


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

Deploy your application:


```python
serve.run(app)
```

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

You can also send the `multiplexed_model_id` using the deployment handle:

```python
handle = serve.get_deployment_handle("ForecastingService", "default")
result = await handle.options(multiplexed_model_id="customer_123").remote(request)
```

### Test concurrent requests

Send concurrent requests to see the LRU caching and model loading in action:


```python
# client_concurrent_requests.py
import random
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

customer_ids = ["customer_123", "customer_456", "customer_789", "customer_abc", "customer_def", "customer_hij"]

def send_request(customer_id):
    live_sequence_data = [random.uniform(90, 110) for _ in range(10)]
    response = requests.post(
        "http://localhost:8000",
        headers={"serve_multiplexed_model_id": customer_id},
        json={"sequence_data": live_sequence_data}
    )
    return customer_id, response.json()["forecast"]

# Create 100 random requests across 6 customers
random_customers = [random.choice(customer_ids) for _ in range(100)]

with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(send_request, cid) for cid in random_customers]
    for future in as_completed(futures):
        customer_id, forecast = future.result()
        print(f"{customer_id}: {forecast[:3]}...")

print("\nSent 100 concurrent requests")
```

Under concurrent load with 6 customers and a cache size of 4, the LRU policy evicts the least recently used models. Watch the logs to see models loading and unloading as requests arrive.

**Note:** If you see frequent model loading/unloading (cache thrashing), increase `max_num_models_per_replica` or add more replicas to warm more models.

### Shutdown

Shutdown your service:


```python
serve.shutdown()
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
(anyscale +6.6s) curl -H "Authorization: Bearer <YOUR-TOKEN>" <YOUR-ENDPOINT>
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
    json={"sequence_data": [100, 102, 98, 105, 110]}
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

Track the ratio of cache hits to total requests using `ray_serve_multiplexed_models_load_counter_total` and `ray_serve_multiplexed_get_model_requests_counter_total`. 
A high cache hit rate (>90%) indicates most requests use cached models. A low rate suggests you need to increase `max_num_models_per_replica` or add more replicas.

See [Monitoring and Debugging](https://docs.ray.io/en/latest/serve/monitoring.html) for more details on monitoring a Serve application.

### Access the dashboard on Anyscale

To view metrics in an Anyscale Service or Workspace:

1. From your console, navigate to your Anyscale Service or Workspace page.
2. Go to the **Metrics** tab, then **Serve Deployment Dashboard**.

From there, you can also open Grafana by clicking **View tab in Grafana**.

## Optimize cache performance

Tune your multiplexed deployment for better cache hit rates and lower latency.

### Increase cache size

Set `max_num_models_per_replica` based on your memory and access patterns to increase the cache size per replica:

```python
# For small models (10MB each) on 4GB nodes
@serve.multiplexed(max_num_models_per_replica=100)

# For larger models (100MB each)
@serve.multiplexed(max_num_models_per_replica=20)
```

You can also scale the number of replicas to keep more models loaded in memory across the cluster:

```python
@serve.deployment(num_replicas=5)  # 5 replicas × 10 models = 50 warm models
```

**Note:** If you observe high latency variance with constant model loading/unloading (cache thrashing), your cache is too small for your access pattern. Increase `max_num_models_per_replica` or add more replicas.

### Pre-warm models at initialization

Avoid cold-starts for high-priority models by pre-loading during initialization:

```python
async def _prewarm(self):
    for customer_id in ["customer_vip_1", "customer_vip_2"]:
        await self.get_model(customer_id)
```

**Note:** First requests for a model are slow because they trigger model loading. Pre-warming eliminates this latency for important models.

## Troubleshooting

### Missing model ID in request

```
ValueError: The model ID cannot be empty.
```

Make sure you pass `serve_multiplexed_model_id` in the header of your request, or in the handle options:
```python
response = requests.post(
    "http://localhost:8000",
    headers={"serve_multiplexed_model_id": "customer_123"},
    json={"sequence_data": sequence_data}
)
# Or use handle options with `multiplexed_model_id`:
await self.forecaster.options(multiplexed_model_id=request.model_id).remote(request)
```

### TypeError: object can't be used in 'await' expression

Make sure you define your model loading function with `async def`:

```python
@serve.multiplexed(max_num_models_per_replica=10)
async def get_model(self, model_id: str):  # Must be async
    ...
```

### Out of memory errors

If models exceed available memory, reduce `max_num_models_per_replica` to limit cache size.

## Summary

This tutorial demonstrated how to use model multiplexing to serve multiple forecasting models. The key concepts covered include:

- Serving multiple models from shared replicas with `@serve.multiplexed`
- Configuring LRU cache size with `max_num_models_per_replica`
- Routing requests with the `serve_multiplexed_model_id` header
- Deploying locally and to production with Anyscale Services
- Monitoring cache usage and model load metrics
