<!--
Do not modify this README. This file is a copy of the notebook and is not used to display the content.
Modify README.ipynb instead, then regenerate this file with:
jupyter nbconvert "content/README.ipynb" --to markdown --output "README.md"
Or use this script: bash convert_to_md.sh
-->

# Model composition for recommendation systems

<div align="left">
<a target="_blank" href="https://console.anyscale.com/template-preview/model_composition_recsys"><img src="https://img.shields.io/badge/🚀 Run_on-Anyscale-9hf"></a>&nbsp;
<a href="https://github.com/ray-project/ray/tree/master/doc/source/serve/tutorials/model_composition_recsys/content" role="button"><img src="https://img.shields.io/static/v1?label=&message=View%20On%20GitHub&color=586069&logo=github&labelColor=2f363d"></a>&nbsp;
</div>

This tutorial shows you how to build a recommendation system using Ray Serve's model composition pattern. Model composition breaks complex ML pipelines into independent deployments that you can scale and update separately.

## Why model composition for recommendation systems?

Recommendation systems typically involve multiple stages: feature extraction, candidate generation, ranking, and filtering. Model composition solves common challenges by:

- **Independent scaling**: Scale feature extractors separately from ranking models based on traffic patterns.
- **Flexible updates**: Update one component without redeploying the entire pipeline.
- **Resource optimization**: Allocate different resources (CPU/GPU) to each component.

See [Model Composition](https://docs.ray.io/en/latest/serve/model-composition.html) for the core concepts and patterns.

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
    autoscaling_config={
        "min_replicas": 1,
        "max_replicas": 5,
        "target_ongoing_requests": 3
    },
    ray_actor_options={"num_cpus": 2}
)
class ItemRankingModel:
    """Ranks items for a user based on features.
    
    In production, this runs a trained ML model (XGBoost, neural network, etc.).
    For this example, the code uses a simple scoring function.
    """
    
    # Mock item catalog. In production, this comes from a database query.
    CANDIDATE_ITEMS = [f"item_{i}" for i in range(1000)]
    
    def __init__(self):
        # In production, this is your cloud storage path or model registry
        # self.model = load_model("/models/ranking_model.pkl")
        pass
    
    def _score_items(self, user_features: Dict[str, float]) -> List[Dict[str, any]]:
        """Score and rank items for a single user."""
        ranked_items = []
        for item_id in self.CANDIDATE_ITEMS:
            item_popularity = (hash(item_id) % 100) / 100.0
            score = (
                user_features["engagement_score"] * 0.6 + 
                item_popularity * 0.4
            )
            ranked_items.append({
                "item_id": item_id,
                "score": round(score, 3)
            })
        ranked_items.sort(key=lambda x: x["score"], reverse=True)
        return ranked_items
    
    @serve.batch(max_batch_size=32, batch_wait_timeout_s=0.01)
    async def rank_items(
        self, 
        user_features_batch: List[Dict[str, float]]
    ) -> List[List[Dict[str, any]]]:
        """Rank candidate items for a batch of users."""
        # Simulate model inference time
        await asyncio.sleep(0.05)
        
        # In production, use vectorized batch inference:
        # return self.model.batch_predict(user_features_batch, self.CANDIDATE_ITEMS)
        
        return [self._score_items(features) for features in user_features_batch]


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
        top_k = data.get("top_k", 5)
        
        # Step 1: Extract user features
        user_features = await self.user_feature_extractor.extract_features.remote(user_id)
        
        # Step 2: Rank candidate items (batched automatically by @serve.batch)
        ranked_items = await self.ranking_model.rank_items.remote(user_features)
        
        # Step 3: Return top-k recommendations
        return {
            "user_id": user_id,
            "recommendations": ranked_items[:top_k]
        }


# Build the application
app = RecommendationService.bind(
    user_feature_extractor=UserFeatureExtractor.bind(),
    ranking_model=ItemRankingModel.bind()
)
```

Each deployment in the composition can scale independently based on its resource needs and traffic patterns. The `RecommendationService` orchestrates calls to the other deployments using deployment handles.

**Note:** The `ItemRankingModel` uses several performance optimizations:
- **Autoscaling**: Automatically scales replicas based on traffic via `autoscaling_config`. See [Autoscaling](https://docs.ray.io/en/latest/serve/autoscaling-guide.html).
- **Request batching**: The `@serve.batch` decorator groups concurrent requests for efficient batch inference. See [Dynamic Request Batching](https://docs.ray.io/en/latest/serve/advanced-guides/dyn-req-batch.html).

**Warning:** When calling deployment handles inside a deployment, always use `await` instead of `.result()`. The `.result()` method blocks the replica from processing other requests while waiting. Using `await` enables the deployment to handle other requests concurrently.

See [Model Composition](https://docs.ray.io/en/latest/serve/model-composition.html) for details on deployment handles and orchestration patterns.

## Deploy locally

Test your composed pipeline on your local machine before moving to production.

### Launch

Deploy your application:


```python
serve.run(app)
```

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
  ]
}
```

The request flows through the pipeline:
1. `RecommendationService` receives the request.
2. `UserFeatureExtractor` extracts user features (~10&nbsp;ms).
3. `ItemRankingModel` scores all candidate items from its catalog (~50&nbsp;ms).
4. `RecommendationService` returns top-k items.

### Test concurrent requests

Send concurrent requests to trigger autoscaling and see the pipeline handle load:


```python
# client_concurrent_requests.py
import requests
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

def send_request(user_id):
    response = requests.post(
        "http://localhost:8000",
        json={"user_id": user_id, "top_k": 3}
    )
    return user_id, response.json()

user_ids = [f"user_{random.randint(1, 1000)}" for _ in range(100)]

with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(send_request, uid) for uid in user_ids]
    for future in as_completed(futures):
        user_id, result = future.result()
        top_items = [rec["item_id"] for rec in result["recommendations"]]
        print(f"{user_id}: {top_items}")

print("\nSent 100 concurrent requests")
```

Under concurrent load, Ray Serve automatically scales the `ItemRankingModel` replicas based on the `autoscaling_config`. When traffic exceeds `target_ongoing_requests` per replica, new replicas spin up (up to `max_replicas`). When traffic drops, replicas scale back down to `min_replicas`.

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

## Summary

In this tutorial, you learned how how to build a recommendation system with Ray Serve using a model composition pattern. You learned to create independent deployments for each pipeline stage, configure autoscaling for changing traffic, orchestrate multi-stage workflows with deployment handles, deploy to production with Anyscale Services, and monitor per-component metrics.
