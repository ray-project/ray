(serve-autoscaling)=

# Ray Serve Autoscaling

Each [Ray Serve deployment](serve-key-concepts-deployment) has one [replica](serve-architecture-high-level-view) by default. This means there is one worker process running the model and serving requests. When traffic to your deployment increases, the single replica can become overloaded. To maintain high performance of your service, you need to scale out your deployment.

## Manual Scaling

Before jumping into autoscaling, which is more complex, the other option to consider is manual scaling. You can increase the number of replicas by setting a higher value for [num_replicas](serve-configure-deployment) in the deployment options through [in place updates](serve-inplace-updates). By default, `num_replicas` is 1. Increasing the number of replicas will horizontally scale out your deployment and improve latency and throughput for increased levels of traffic.

```yaml
# Deploy with a single replica
deployments:
- name: Model
  num_replicas: 1

# Scale up to 10 replicas
deployments:
- name: Model
  num_replicas: 10
```

## Autoscaling Basic Configuration

Instead of setting a fixed number of replicas for a deployment and manually updating it, you can configure a deployment to autoscale based on incoming traffic. The Serve autoscaler reacts to traffic by monitoring metrics like queue sizes or queries per second (QPS) and making scaling decisions to add or remove replicas. Turn on autoscaling for a deployment by setting `num_replicas="auto"`. You can further configure it by tuning the [autoscaling_config](../serve/api/doc/ray.serve.config.AutoscalingConfig.rst) in deployment options.

By default, autoscaling is based on the number of ongoing requests per replica. The following config is what we will use in the example in the following section.
```yaml
- name: Model
  num_replicas: auto
```

Setting `num_replicas="auto"` uses the default `replica_queue_length_autoscaling_policy` and is equivalent to the following deployment configuration.
```yaml
- name: Model
  max_ongoing_requests: 5 # This is a deployment-level setting, not part of autoscaling_config
  autoscaling_config:
    policy: "replica_queue_length_autoscaling_policy" # Default policy
    target_ongoing_requests: 2.0
    min_replicas: 1
    max_replicas: 100
    # Default values for other fields like metrics_interval_s, look_back_period_s, etc.
```
:::{note}
You can set `num_replicas="auto"` and override its default values (shown above) by specifying `autoscaling_config`, or you can omit `num_replicas="auto"` and fully configure autoscaling yourself.
:::

Let's dive into what each of these parameters do for the default policy:

* **`target_ongoing_requests`**: The average number of ongoing requests per replica that the Serve autoscaler tries to ensure. You can adjust it based on your request processing length (the longer the requests, the smaller this number should be) as well as your latency objective (the shorter you want your latency to be, the smaller this number should be).
* **`max_ongoing_requests`** (deployment option, not autoscaling_config): The maximum number of ongoing requests allowed for a replica. It's important to set it relative to the `target_ongoing_requests` value if you turn on autoscaling for your deployment.
* **`min_replicas`**: The minimum number of replicas for the deployment. Set this to 0 if there are long periods of no traffic and some extra tail latency during upscale is acceptable. Otherwise, set this to what you think you need for low traffic.
* **`max_replicas`**: The maximum number of replicas for the deployment. Set this to ~20% higher than what you think you need for peak traffic.

These guidelines are a great starting point for the default policy. If you decide to further tune your autoscaling config for your application, see [Advanced Ray Serve Autoscaling](serve-advanced-autoscaling).

## QPS-Based Autoscaling

Ray Serve also offers an autoscaling policy based on Queries Per Second (QPS). This policy adjusts the number of replicas to try and maintain a target QPS per replica. This can be particularly useful when your workload's processing time per request varies significantly, making the number of ongoing requests a less stable metric for load.

### Configuration for QPS-Based Autoscaling

To use the QPS-based autoscaling policy, you need to configure the following fields in the `autoscaling_config`:

*   **`policy` (str)**: Set this to `"qps_based_autoscaling_policy"` to enable this policy. The default is `"replica_queue_length_autoscaling_policy"`.
*   **`target_qps_per_replica` (float)**: This is the target QPS that Serve will attempt to maintain for each replica. For example, if this is set to 100.0, and the observed QPS for the deployment is 500.0, the autoscaler will aim for 5 replicas. This field is required if `policy` is set to `"qps_based_autoscaling_policy"`, and it must be a positive value.
*   **`qps_autoscaling_window_s` (float)**: The time window in seconds over which the incoming QPS is averaged. For example, if set to `30.0`, the autoscaler will consider the average QPS over the last 30 seconds to make scaling decisions. Defaults to 10.0 seconds. Must be a positive value.

Other existing `autoscaling_config` fields like `min_replicas`, `max_replicas`, `upscale_delay_s`, and `downscale_delay_s` work the same way as they do for the default policy.

### Examples for QPS-Based Autoscaling

Here's how you can configure QPS-based autoscaling in your deployment:

::::{tab-set}

:::{tab-item} YAML Example
```yaml
# serve_config.yaml
applications:
- name: MyQPSApp
  import_path: my_module:my_app # Replace with your application's import path
  deployments:
  - name: Model
    autoscaling_config:
      policy: "qps_based_autoscaling_policy"
      min_replicas: 1
      max_replicas: 10
      target_qps_per_replica: 50.0       # Target 50 QPS for each replica
      qps_autoscaling_window_s: 30.0     # Average QPS over the last 30 seconds
      upscale_delay_s: 60.0              # Wait 60s before scaling up further
      downscale_delay_s: 300.0           # Wait 300s before scaling down further
```
:::

:::{tab-item} Python API Example
```python
from ray import serve
from ray.serve.config import AutoscalingConfig

@serve.deployment(
    autoscaling_config=AutoscalingConfig(
        policy="qps_based_autoscaling_policy",
        min_replicas=1,
        max_replicas=10,
        target_qps_per_replica=50.0,       # Target 50 QPS for each replica
        qps_autoscaling_window_s=30.0,     # Average QPS over the last 30 seconds
        upscale_delay_s=60.0,              # Wait 60s before scaling up further
        downscale_delay_s=300.0,           # Wait 300s before scaling down further
    )
)
class MyModel:
    async def __call__(self, request):
        # Your model inference logic here
        return {"result": "ok"}

# Example: app = MyModel.bind()
# You can then deploy this with `serve.run(app)` or include it in a Serve config file.
```
:::
::::

### When to Use QPS-Based Autoscaling

The QPS-based autoscaling policy can be beneficial in several scenarios:

*   **Variable Request Processing Times**: If the time taken to process individual requests varies significantly (e.g., due to input data complexity or external service calls), the number of "ongoing requests" might not accurately reflect the true load on the system. QPS provides a more direct measure of throughput.
*   **Throughput-Oriented Scaling**: When your primary scaling concern is to maintain a certain level of throughput (queries per second) per replica, this policy is a natural fit.
*   **More Stable Scaling**: In some cases, QPS can be a more stable metric than ongoing requests, especially if request latency is not strongly correlated with system load but rather with other factors. This can lead to more predictable scaling behavior.

In contrast, the default `replica_queue_length_autoscaling_policy` (based on `target_ongoing_requests`) is often effective for CPU-bound workloads where request processing times are relatively uniform and the number of requests actively being processed is a good proxy for load.

Choose the policy that best reflects the performance characteristics and scaling requirements of your specific application.

(resnet-autoscaling-example)=
## Basic example

This example is a synchronous workload that runs ResNet50. The application code and its autoscaling configuration are below. Alternatively, see the second tab for specifying the autoscaling config through a YAML file.

::::{tab-set}

:::{tab-item} Application Code
```{literalinclude} doc_code/resnet50_example.py
:language: python
:start-after: __serve_example_begin__
:end-before: __serve_example_end__
```
:::

:::{tab-item} (Alternative) YAML config

```yaml
applications:
  - name: default
    import_path: resnet:app
    deployments:
    - name: Model
      num_replicas: auto
```

:::
::::

This example uses [Locust](https://locust.io/) to run a load test against this application. The Locust load test runs a certain number of "users" that ping the ResNet50 service, where each user has a [constant wait time](https://docs.locust.io/en/stable/writing-a-locustfile.html#wait-time-attribute) of 0. Each user (repeatedly) sends a request, waits for a response, then immediately sends the next request. The number of users running over time is shown in the following graph:

![users](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/resnet50_users.png)

The results of the load test are as follows:

|  |  |  |
| -------- | --- | ------- |
| Replicas | <img src="https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/resnet50_replicas.png" alt="replicas" width="600"/> |
| QPS | <img src="https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/resnet50_rps.png" alt="qps"/> |
| P50 Latency | <img src="https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/resnet50_latency.png" alt="latency"/> |

Notice the following:
- Each Locust user constantly sends a single request and waits for a response. As a result, the number of autoscaled replicas is roughly half the number of Locust users over time as Serve attempts to satisfy the `target_ongoing_requests=2` setting.
- The throughput of the system increases with the number of users and replicas.
- The latency briefly spikes when traffic increases, but otherwise stays relatively steady.

## Ray Serve Autoscaler vs Ray Autoscaler

The Ray Serve Autoscaler is an application-level autoscaler that sits on top of the [Ray Autoscaler](cluster-index).
Concretely, this means that the Ray Serve autoscaler asks Ray to start a number of replica actors based on the request demand.
If the Ray Autoscaler determines there aren't enough available resources (e.g. CPUs, GPUs, etc.) to place these actors, it responds by requesting more Ray nodes.
The underlying cloud provider then responds by adding more nodes.
Similarly, when Ray Serve scales down and terminates replica Actors, it attempts to make as many nodes idle as possible so the Ray Autoscaler can remove them. To learn more about the architecture underlying Ray Serve Autoscaling, see [Ray Serve Autoscaling Architecture](serve-autoscaling-architecture).
