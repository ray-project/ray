---
orphan: true
---

<!--
Do not modify this README. This file is a copy of the notebook and is not used to display the content.
Modify notebook.ipynb instead, then regenerate this file with:
jupyter nbconvert "content/notebook.ipynb" --to markdown --output "README.md"
Or use this script: bash convert_to_md.sh
-->

# Model composition for recommendation systems

<div align="left">
<a target="_blank" href="https://console.anyscale.com/template-preview/model_composition_recsys"><img src="https://img.shields.io/badge/🚀 Run_on-Anyscale-9hf"></a>&nbsp;
<a href="https://github.com/ray-project/ray/tree/master/doc/source/serve/tutorials/model_composition_recsys/content" role="button"><img src="https://img.shields.io/static/v1?label=&message=View%20On%20GitHub&color=586069&logo=github&labelColor=2f363d"></a>&nbsp;
</div>

This tutorial shows you how to build a recommendation system using Ray Serve's model composition pattern. Model composition lets you break complex ML pipelines into independent deployments that scale and update separately.

## Why model composition for recommendation systems?

Recommendation systems typically involve multiple stages: feature extraction, candidate generation, ranking, and filtering. Model composition solves common challenges by:

- **Independent scaling**: Scale feature extractors separately from ranking models based on traffic patterns.
- **Team ownership**: Different teams can own and deploy their models independently.
- **Flexible updates**: Update one component without redeploying the entire pipeline.
- **Resource optimization**: Allocate different resources (CPU/GPU) to each component.

See [Model Composition](https://docs.ray.io/en/latest/serve/model-composition.html) for the core concepts and patterns.

Use model composition when you have:
- Multi-stage ML pipelines with distinct models.
- Components that scale differently (for example lightweight feature extraction vs. heavy ranking).
- Multiple teams contributing models to the same system.
- Need to A/B test individual components.

## Configure a composed deployment

Build a recommendation system with three components:
1. **UserFeatureExtractor**: Extracts user features (demographics, history, preferences).
2. **ItemRankingModel**: Scores items based on user features.
3. **RecommendationService**: Orchestrates the pipeline and returns top recommendations.



```python
# serve_recommendation_pipeline.py
import asyncio
import numpy as np
from typing import List, Dict
from ray import serve
from ray.serve.handle import DeploymentHandle
from starlette.requests import Request


# Component 1: User Feature Extractor
@serve.deployment(num_replicas=2)
class UserFeatureExtractor:
    """Extracts user features from user ID.
    
    In production, this queries a database or feature store.
    For this example, the code generates mock features.
    """
    
    async def extract_features(self, user_id: str) -> Dict[str, float]:
        """Extract user features."""
        # Simulate database lookup
        await asyncio.sleep(0.01)
        
        # In production:
        # features = await db.query("SELECT * FROM user_features WHERE user_id = ?", user_id)
        # return features
        
        # Mock features based on user_id hash
        np.random.seed(hash(user_id) % 10000)
        return {
            "age_group": float(np.random.randint(18, 65)),
            "avg_session_duration": float(np.random.uniform(5, 60)),
            "total_purchases": float(np.random.randint(0, 100)),
            "engagement_score": float(np.random.uniform(0, 1)),
        }


# Component 2: Item Ranking Model
@serve.deployment(
    num_replicas=3,
    ray_actor_options={"num_cpus": 2}
)
class ItemRankingModel:
    """Ranks items for a user based on features.
    
    In production, this runs a trained ML model (XGBoost, neural network, etc.).
    For this example, the code uses a simple scoring function.
    """
    
    def __init__(self):
        # In production: Load model weights
        # self.model = load_model("s3://models/ranking_model.pkl")
        pass
    
    async def rank_items(
        self, 
        user_features: Dict[str, float], 
        candidate_items: List[str]
    ) -> List[Dict[str, any]]:
        """Rank candidate items for the user."""
        # Simulate model inference time
        await asyncio.sleep(0.05)
        
        # In production:
        # scores = self.model.predict(user_features, candidate_items)
        
        # Mock scoring: combine user engagement with item popularity
        ranked_items = []
        for item_id in candidate_items:
            # Simple mock scoring based on user engagement and item hash
            item_popularity = (hash(item_id) % 100) / 100.0
            score = (
                user_features["engagement_score"] * 0.6 + 
                item_popularity * 0.4
            )
            ranked_items.append({
                "item_id": item_id,
                "score": round(score, 3)
            })
        
        # Sort by score descending
        ranked_items.sort(key=lambda x: x["score"], reverse=True)
        return ranked_items


# Component 3: Recommendation Service (Orchestrator)
@serve.deployment
class RecommendationService:
    """Orchestrates the recommendation pipeline."""
    
    def __init__(
        self,
        user_feature_extractor: DeploymentHandle,
        ranking_model: DeploymentHandle
    ):
        self.user_feature_extractor = user_feature_extractor
        self.ranking_model = ranking_model
    
    async def __call__(self, request: Request) -> Dict:
        """Generate recommendations for a user."""
        data = await request.json()
        user_id = data["user_id"]
        candidate_items = data.get("candidate_items", [])
        top_k = data.get("top_k", 5)
        
        # Step 1: Extract user features
        user_features = await self.user_feature_extractor.extract_features.remote(user_id)
        
        # Step 2: Rank candidate items
        ranked_items = await self.ranking_model.rank_items.remote(
            user_features, 
            candidate_items
        )
        
        # Step 3: Return top-k recommendations
        return {
            "user_id": user_id,
            "recommendations": ranked_items[:top_k],
            "total_candidates": len(candidate_items)
        }


# Build the application
app = RecommendationService.bind(
    user_feature_extractor=UserFeatureExtractor.bind(),
    ranking_model=ItemRankingModel.bind()
)

```

Each deployment in the composition can scale independently based on its resource needs and traffic patterns. The `RecommendationService` orchestrates calls to the other deployments using deployment handles.

See [Model Composition](https://docs.ray.io/en/latest/serve/model-composition.html) for details on deployment handles and orchestration patterns.

## Deploy locally

Test your composed pipeline on your local machine before moving to production.

### Launch

In a terminal, run:



```bash
serve run serve_recommendation_pipeline:app --non-blocking
```

**Note:** When running in a notebook, the `--non-blocking` flag returns control immediately so you can continue executing cells. Without it, `serve run` blocks the notebook. In a terminal, you can omit this flag to stream logs to the console.

Ray Serve logs the endpoint of your application once the service is deployed:
```console
INFO 2025-12-04 03:15:42,123 serve 8923 -- Application 'default' is ready at http://0.0.0.0:8000/
```

### Send requests

Send a recommendation request for a user:



```python
# client.py
import requests

response = requests.post(
    "http://localhost:8000",
    json={
        "user_id": "user_42",
        "candidate_items": [
            "item_101", "item_102", "item_103", 
            "item_104", "item_105", "item_106",
            "item_107", "item_108", "item_109", "item_110"
        ],
        "top_k": 5
    }
)

print(response.json())

```

Output:
```json
{
  "user_id": "user_42",
  "recommendations": [
    {"item_id": "item_108", "score": 0.847},
    {"item_id": "item_103", "score": 0.792},
    {"item_id": "item_110", "score": 0.756},
    {"item_id": "item_101", "score": 0.723},
    {"item_id": "item_105", "score": 0.689}
  ],
  "total_candidates": 10
}
```

The request flows through the pipeline:
1. `RecommendationService` receives the request.
2. `UserFeatureExtractor` extracts user features (~10&nbsp;ms).
3. `ItemRankingModel` scores all candidate items (~50&nbsp;ms).
4. `RecommendationService` returns top-k items.

### Test with multiple users

Send requests for different users to see the pipeline in action:


```python
# client_multiple_requests.py
import requests
import random

# Generate sample candidate items
all_items = [f"item_{i}" for i in range(1000)]

# Test with multiple users
for i in range(100):
    user_id = f"user_{random.randint(1, 1000)}"
    # Each user gets a random subset of candidate items
    candidate_items = random.sample(all_items, k=50)
    
    response = requests.post(
        "http://localhost:8000",
        json={
            "user_id": user_id,
            "candidate_items": candidate_items,
            "top_k": 3
        }
    )
    
    result = response.json()
    top_items = [rec["item_id"] for rec in result["recommendations"]]
    print(f"Request {i+1} - {user_id}: {top_items}")

print(f"\nSent 100 requests total")

```

Each component processes requests independently and can scale based on load. For example:
- `UserFeatureExtractor` (2 replicas) handles feature extraction.
- `ItemRankingModel` (3 replicas with 2 CPUs each) handles compute-intensive ranking.
- `RecommendationService` (1 replica) orchestrates the pipeline.

### Shutdown

Shutdown your service:


```bash
serve shutdown -y
```

## Deploy to production with Anyscale Services

For production deployment, use Anyscale Services to deploy the application to a dedicated cluster.

Create a `service.yaml` file:

```yaml
# service.yaml
name: my-recommendation-service
image_uri: anyscale/ray:2.52.1-slim-py312
compute_config:
  auto_select_worker_config: true
working_dir: .
applications:
  - import_path: serve_recommendation_pipeline:app

```

### Launch

Deploy your Anyscale Service:
```bash
anyscale service deploy -f service.yaml
```

The output shows your endpoint URL and authentication token.
```console
(anyscale +5.2s) Query the service once it's running using the following curl command:
(anyscale +5.2s) curl -H "Authorization: Bearer <YOUR-TOKEN>" <YOUR-ENDPOINT>
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
    headers={"Authorization": f"Bearer {TOKEN}"},
    json={
        "user_id": "user_42",
        "candidate_items": [f"item_{i}" for i in range(100, 120)],
        "top_k": 5
    }
)

print(response.json())

```

### Shutdown

Terminate your Anyscale Service:
```bash
anyscale service terminate -n my-recommendation-service
```

## Monitor your deployment

Ray Serve exposes per-deployment metrics that help you understand pipeline performance:

| Metric | Description |
|--------|-------------|
| `ray_serve_deployment_request_counter` | Total requests per deployment |
| `ray_serve_deployment_processing_latency_ms` | Processing time per replica |
| `ray_serve_num_deployment_http_error_requests_total` | Error rate per deployment |
| `ray_serve_deployment_queued_queries` | Queue depth per replica |

See [Monitoring and Debugging](https://docs.ray.io/en/latest/serve/monitoring.html) for more details on monitoring a Serve application.

### Access the dashboard on Anyscale

To view metrics in an Anyscale Service or Workspace:

1. From your console, navigate to your Anyscale Service or Workspace page.
2. Go to the **Metrics** tab, then **Serve Deployment Dashboard**.

From there, you can also open Grafana by clicking **View tab in Grafana**.

## Performance tips

Optimize your composed pipeline for better throughput and lower latency.

### Scale deployments independently

Identify bottlenecks and scale specific components:

```python
# Scale feature extraction for high traffic
@serve.deployment(num_replicas=5)
class UserFeatureExtractor:
    ...

# Scale ranking model with more resources
@serve.deployment(
    num_replicas=10,
    ray_actor_options={"num_cpus": 4, "num_gpus": 1}
)
class ItemRankingModel:
    ...
```

### Use autoscaling

Configure autoscaling for components with variable load:

```python
@serve.deployment(
    autoscaling_config={
        "min_replicas": 2,
        "max_replicas": 10,
        "target_ongoing_requests": 5
    }
)
class ItemRankingModel:
    ...
```

### Batch requests for ranking models

Improve throughput by batching ranking requests:

```python
@serve.deployment
class ItemRankingModel:
    @serve.batch(max_batch_size=32, batch_wait_timeout_s=0.01)
    async def rank_items(self, requests):
        # Process batch of requests together
        ...
```

### Use co-routines for blocking operations

Use async operations for blocking operations:

```python
@serve.deployment
class UserFeatureExtractor:
    async def extract_features(self, user_id: str):
        # Use async database client
        features = await db.query_async(user_id)
        return features
```

:::warning
When calling deployment handles inside a deployment, always use `await` instead of `.result()`. The `.result()` method blocks the deployment from processing other requests while waiting for the remote call to finish. Using `await` lets the deployment handle other requests concurrently:

```python
@serve.deployment
class RecommendationService:
    def __init__(self, user_feature_extractor: DeploymentHandle):
        self.user_feature_extractor = user_feature_extractor
    
    async def __call__(self, request):
        user_id = (await request.json())["user_id"]
        
        # Correct: Non-blocking - allows other requests to be processed
        features = await self.user_feature_extractor.extract_features.remote(user_id)
        
        # Avoid: Blocks the replica from handling other requests
        # features = self.user_feature_extractor.extract_features.remote(user_id).result()
        
        return features
```
:::

## Troubleshooting

Common issues you might encounter when using model composition.

### Missing deployment handle

```
TypeError: __init__() missing 1 required positional argument: 'ranking_model'
```

Make sure you bind all deployment handles when building the application:

```python
# Correct: All handles bound
app = RecommendationService.bind(
    user_feature_extractor=UserFeatureExtractor.bind(),
    ranking_model=ItemRankingModel.bind()
)

# Incorrect: Missing ranking_model handle
app = RecommendationService.bind(
    user_feature_extractor=UserFeatureExtractor.bind()
)
```

### AttributeError: 'DeploymentHandle' object has no attribute 'X'

```
AttributeError: 'DeploymentHandle' object has no attribute 'extract_features'
```

Make sure you call methods on the handle using `.remote()`:

```python
# Correct
features = await self.user_feature_extractor.extract_features.remote(user_id)

# Incorrect
features = await self.user_feature_extractor.extract_features(user_id)
```

### Timeout errors

```
RayTaskError: Task timed out after 30 seconds
```

Check for deadlocks or increase timeout for slow components such as model inference or database operations:

```python
# Set `max_ongoing_requests` to monitor which replica is not receiving responses fast enough
@serve.deployment(max_ongoing_requests=10)
class ItemRankingModel:
    ...

# Configure timeout per call
ranked = await self.ranking_model.rank_items.options(
    timeout_s=60
).remote(features, items)
```

### High latency between components

If you observe high latency and low throughput between deployments:

1. **Check for `.result()` usage**: Make sure you're using `await` instead of `.result()` when calling deployment handles. Using `.result()` blocks the replica from processing other requests, which severely impacts performance. See [Use co-routines for blocking operations](#use-co-routines-for-blocking-operations) for the correct pattern.
2. **Check replica placement**: Ensure replicas are on the same nodes when possible.
3. **Monitor queue depth**: High queue depth indicates insufficient replicas.
4. **Profile each component**: Identify which stage is the bottleneck.
5. **Consider batching**: Batch requests to improve throughput.

## Summary

This tutorial shows you how to use model composition to build recommendation systems with independent deployments, configure each component with different resources and scaling policies, orchestrate multi-stage pipelines with deployment handles, deploy both locally and in production, monitor per-component metrics, optimize pipeline performance, and troubleshoot common issues.

