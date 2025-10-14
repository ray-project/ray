(serve-external-scale-webhook)=

:::{warning}
This API is in alpha and may change before becoming stable.
:::

# External Scaling Webhook

Ray Serve exposes a REST API endpoint that you can use to dynamically scale your deployments from outside the Ray cluster. This endpoint gives you flexibility to implement custom scaling logic based on any metrics or signals you choose, such as external monitoring systems, business metrics, or predictive models.

## Overview

The external scaling webhook provides programmatic control over the number of replicas for any deployment in your Ray Serve application. Unlike Ray Serve's built-in autoscaling, which scales based on queue depth and ongoing requests, this webhook allows you to scale based on any external criteria you define.

## Prerequisites

Before you can use the external scaling webhook, you must enable it in your Ray Serve application configuration:

### Enable external scaler

Set `external_scaler_enabled: true` in your application configuration:

```yaml
applications:
  - name: my-app
    import_path: my_module:app
    external_scaler_enabled: true
    deployments:
      - name: my-deployment
        num_replicas: 1
```

:::{warning}
External scaling and built-in autoscaling are mutually exclusive. You can't use both for the same application.

- If you set `external_scaler_enabled: true`, you **must not** configure `autoscaling_config` on any deployment in that application.
- If you configure `autoscaling_config` on any deployment, you **must not** set `external_scaler_enabled: true` for the application.

Attempting to use both will result in an error.
:::

### Get authentication token

The external scaling webhook requires authentication using a bearer token. You can obtain this token from the Ray Dashboard UI:

1. Open the Ray Dashboard in your browser (typically at `http://localhost:8265`).
2. Navigate to the Serve section.
3. Find and copy the authentication token for your application.

## API endpoint

The webhook is available at the following endpoint:

```
POST /api/v1/applications/{application_name}/deployments/{deployment_name}/scale
```

**Path Parameters:**
- `application_name`: The name of your Serve application.
- `deployment_name`: The name of the deployment you want to scale.

**Headers:**
- `Authorization` (required): Bearer token for authentication. Format: `Bearer <token>`
- `Content-Type` (required): Must be `application/json`

**Request Body:**

The following example shows the request body structure:

```json
{
    "target_num_replicas": 5
}
```

The request body must conform to the [`ScaleDeploymentRequest`](https://docs.ray.io/en/latest/serve/api/doc/ray.serve.schema.ScaleDeploymentRequest.html) schema:

- `target_num_replicas` (integer, required): The target number of replicas for the deployment. Must be a non-negative integer.


## Example - Predictive scaling

Implement predictive scaling based on historical patterns or forecasts. For instance, you can preemptively scale up before anticipated traffic spikes:

```python
import requests
from datetime import datetime

def predictive_scale(
    application_name: str,
    deployment_name: str,
    auth_token: str,
    serve_endpoint: str = "http://localhost:8000"
) -> bool:
    """Scale based on time of day and historical patterns."""
    hour = datetime.now().hour
    
    # Define scaling profile based on historical traffic patterns
    if 9 <= hour < 17:  # Business hours
        target_replicas = 10
    elif 17 <= hour < 22:  # Evening peak
        target_replicas = 15
    else:  # Off-peak hours
        target_replicas = 3
    
    url = (
        f"{serve_endpoint}/api/v1/applications/{application_name}"
        f"/deployments/{deployment_name}/scale"
    )
    
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json"
    }
    
    response = requests.post(
        url,
        headers=headers,
        json={"target_num_replicas": target_replicas}
    )
    
    return response.status_code == 200

```

## Use cases

The external scaling webhook is useful for several scenarios where you need custom scaling logic beyond what Ray Serve's built-in autoscaling provides:

### Custom metric-based scaling

Scale your deployments based on business or application metrics that Ray Serve doesn't track automatically:

- External monitoring systems such as Prometheus, Datadog, or CloudWatch metrics.
- Database query latencies or connection pool sizes.
- Cost metrics to optimize for budget constraints.

### Predictive and scheduled scaling

Implement predictive scaling based on historical patterns or business schedules:

- Preemptive scaling before anticipated traffic spikes (such as daily or weekly patterns).
- Event-driven scaling for known traffic events (such as sales, launches, or scheduled batch jobs).
- Time-of-day based scaling profiles for predictable workloads.

### Manual and operational control

Direct control over replica counts for operational scenarios:

- Manual scaling for load testing or performance testing.
- Cost optimization by scaling down during off-peak hours or weekends.
- Development and staging environment management.

