---
myst:
  html_meta:
    description: "Advanced Ray Serve autoscaling: tune autoscaling_config for steady state and limits, control reaction to traffic, and scale composed models."
---

(serve-advanced-autoscaling)=

# Advanced Ray Serve autoscaling

This guide covers the advanced autoscaling parameters in [`autoscaling_config`](../api/doc/ray.serve.config.AutoscalingConfig.rst) and walks through a model composition example.


(serve-autoscaling-config-parameters)=
## Autoscaling config parameters

This section explains Serve autoscaling concepts in more detail and shows how to set your autoscaling config.

### Required: Define the steady state of your system

To define the steady state you want for your deployments, set values for `target_ongoing_requests` and `max_ongoing_requests`.

#### [`target_ongoing_requests`](../api/doc/ray.serve.config.AutoscalingConfig.rst) [default=2]

:::{note}
The default for `target_ongoing_requests` changed from 1.0 to 2.0 in Ray 2.32.0. To override the default, set it manually.
:::

Serve scales the number of replicas for a deployment up or down based on the average number of ongoing requests per replica. Serve compares the *actual* number of ongoing requests per replica with the target value in your autoscaling config and makes upscale or downscale decisions from that comparison. Set the target value with `target_ongoing_requests`, and Serve attempts to keep roughly that number of requests in progress or waiting in the queue at each replica.

Always load test your workloads. For example, if your use case is latency sensitive, you can lower `target_ongoing_requests` to maintain high performance. Benchmark your application code and set this number based on an end-to-end latency objective.

:::{note}
For example, suppose you have two replicas of a synchronous deployment that has 100ms latency, serving a traffic load of 30 QPS. Serve assigns requests to the replicas faster than the replicas can finish processing them. Over time, more and more requests queue up at each replica, and the average number of ongoing requests per replica steadily increases. These queued requests count as ongoing requests. Latency also increases because new requests have to wait for old requests to finish processing. If you set `target_ongoing_requests = 1`, Serve detects more ongoing requests per replica than the target and adds replicas. At three replicas, your system can process 30 QPS with one ongoing request per replica on average.
:::

#### `max_ongoing_requests` [default=5]

:::{note}
The default for `max_ongoing_requests` changed from 100 to 5 in Ray 2.32.0. To override the default, set it manually.
:::

Proxies also respect a maximum queue limit when they assign requests to replicas. Define the limit with `max_ongoing_requests`, and set it about 20% to 50% higher than `target_ongoing_requests`.

- Setting it too low can throttle throughput. Instead of the proxy forwarding them to replicas for concurrent execution, requests tend to queue up at the proxy and wait for replicas to finish processing existing requests.

  :::{note}
  Tune `max_ongoing_requests` higher, especially for lightweight requests. Otherwise, overall throughput suffers.
  :::

- Setting it too high can lead to imbalanced routing and high tail latencies during upscale. When the autoscaler scales a deployment up in response to a traffic spike, Serve might assign most or all of the requests to the existing replicas before the new replicas start.


### Required: Define upper and lower autoscaling limits

To use autoscaling, define the minimum and maximum number of resources allowed for your system.

* **[`min_replicas`](../api/doc/ray.serve.config.AutoscalingConfig.rst) [default=1]**: The minimum number of replicas for the deployment. To ensure your system can handle a certain level of traffic at all times, set `min_replicas` to a positive number. If you anticipate periods of no traffic and want to scale to zero to save cost, set `min_replicas = 0`. Setting `min_replicas = 0` causes higher tail latencies. When you start sending traffic, the deployment scales up, and there's a cold start time while Serve waits for replicas to start and serve the request.

* **[`max_replicas`](../api/doc/ray.serve.config.AutoscalingConfig.rst) [default=1]**: The maximum number of replicas for the deployment. This value should be greater than `min_replicas`. Ray Serve autoscaling relies on the Ray autoscaler to add nodes when the cluster's available resources, such as CPUs and GPUs, aren't enough to support more replicas.

* **[`initial_replicas`](../api/doc/ray.serve.config.AutoscalingConfig.rst)**: The number of replicas that Serve starts initially for the deployment. Defaults to the value of `min_replicas`.


### Optional: Define how the system reacts to changing traffic

With a steady stream of traffic and appropriately configured `min_replicas` and `max_replicas`, the value you choose for `target_ongoing_requests` essentially fixes the steady state of your system. Before your system reaches steady state, it reacts to traffic shifts. How you want it to react to changes in traffic determines how to set the remaining autoscaling parameters.

* **[`upscale_delay_s`](../api/doc/ray.serve.config.AutoscalingConfig.rst) [default=30s]**: How long Serve waits before scaling up the number of replicas in your deployment. In other words, this parameter controls the frequency of upscale decisions. If the replicas *consistently* serve more requests than desired for `upscale_delay_s` seconds, Serve scales up the number of replicas based on aggregated ongoing requests metrics. For example, if your service is likely to experience bursts of traffic, you can lower `upscale_delay_s` so that your application reacts quickly to increases in traffic.

For more granular control over when Serve removes replicas, you can set different delays for different downscaling scenarios. This is particularly useful when you want scaling down to zero to behave differently from scaling down to a nonzero number of replicas.

* **[`downscale_delay_s`](../api/doc/ray.serve.config.AutoscalingConfig.rst) [default=600s]**: How long Serve waits before scaling down the number of replicas in your deployment. If the replicas *consistently* serve fewer requests than desired for `downscale_delay_s` seconds, Serve scales down the number of replicas based on aggregated ongoing requests metrics. This delay applies to all downscaling decisions except the optional transition from one replica to zero, which the next parameter covers. For example, if your application initializes slowly, you can increase `downscale_delay_s` to downscale less often and avoid reinitialization costs when the application needs to upscale again.

* **[`downscale_to_zero_delay_s`](../api/doc/ray.serve.config.AutoscalingConfig.rst) [Optional]**: How long Serve waits before scaling from one replica down to zero. This parameter applies only when `min_replicas = 0`. If you don't set it, the transition from one replica to zero uses the `downscale_delay_s` value. This is useful when you want more conservative scale-to-zero behavior. For example, you might set `downscale_delay_s = 300` for regular downscaling but `downscale_to_zero_delay_s = 1800` to wait 30 minutes before scaling to zero, so brief periods of inactivity don't cause cold starts.

* **[`upscale_smoothing_factor`](../api/doc/ray.serve.config.AutoscalingConfig.rst) [default_value=1.0]**: Deprecated. This parameter is renamed to `upscaling_factor`, and a future release will remove it.

* **[`downscale_smoothing_factor`](../api/doc/ray.serve.config.AutoscalingConfig.rst) [default_value=1.0]**: Deprecated. This parameter is renamed to `downscaling_factor`, and a future release will remove it.

* **[`upscaling_factor`](../api/doc/ray.serve.config.AutoscalingConfig.rst) [default_value=1.0]**: The multiplicative factor that amplifies or moderates each upscaling decision. For example, when the application has high traffic volume in a short period of time, you can increase `upscaling_factor` to scale up resources quickly. This parameter acts like a "gain" factor that amplifies the response of the autoscaling algorithm.

* **[`downscaling_factor`](../api/doc/ray.serve.config.AutoscalingConfig.rst) [default_value=1.0]**: The multiplicative factor that amplifies or moderates each downscaling decision. For example, if you want your application to be less sensitive to drops in traffic and scale down more conservatively, you can decrease `downscaling_factor` to slow the pace of downscaling.

* **[`metrics_interval_s`](../api/doc/ray.serve.config.AutoscalingConfig.rst) [default_value=10]**: How often each replica and handle reports its ongoing requests to the autoscaler. A future release will remove this deployment-level config in favor of a cross-application global config.

  :::{note}
  If replicas and handles report metrics infrequently, Ray Serve can take longer to notice a change in autoscaling metrics, so scaling can start later even when your delays are short. For example, if you set `upscale_delay_s = 3` but replicas and handles push metrics every 10 seconds, Ray Serve might not see a change until the next push, which can limit scaling up to about once every 10 seconds.
  :::

  :::{warning}
  Setting `metrics_interval_s` to an extremely low value, such as `0.1`, causes replicas and handles to push metrics to the controller at high frequency. This can overload the controller on the head node and cause instability. Use the default value of `10` unless you have a specific, well-tested reason to lower it.
  :::

* **[`look_back_period_s`](../api/doc/ray.serve.config.AutoscalingConfig.rst) [default_value=30]**: The window over which Serve calculates the average number of ongoing requests per replica.

* **[`aggregation_function`](../api/doc/ray.serve.config.AutoscalingConfig.rst) [default_value="mean"]**: How Serve aggregates metrics over the `look_back_period_s` time window. The aggregation function determines how Ray Serve combines multiple metric measurements into a single value for autoscaling decisions. Ray Serve supports three values:
  - `"mean"`: The default. Uses a time-weighted average of metrics. This average produces smooth scaling behavior that responds to sustained traffic patterns.
  - `"max"`: Uses the highest value the deployment total reached during the window, not the highest value of any one replica. This makes autoscaling more sensitive to spikes.
  - `"min"`: Uses the lowest value the deployment total reached during the window. This scales conservatively. Because an idle stretch inside the window pins the minimum to zero, a deployment that scaled to zero stays there until those samples age out.

For most workloads, the default `"mean"` aggregation provides the best balance. Use `"max"` if you need to react quickly to traffic spikes, or `"min"` if you prefer conservative scaling that avoids rapid fluctuations.

(how-autoscaling-metrics-work)=
### How do autoscaling metrics work?

Understanding how metrics flow through the autoscaling system helps you configure the parameters effectively. The metrics pipeline has four stages, each with its own timing parameters:

```
┌──────────────────────────────────────────────────────────────────────────┐
│  Metrics Pipeline Overview                                               │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  Replicas/Handles         Controller             Autoscaling Policy      │
│  ┌──────────┐             ┌──────────┐           ┌──────────┐            │
│  │ Record   │   Push      │ Receive  │  Decide   │ Policy   │            │
│  │ Metrics  │────────────>│ Metrics  │──────────>│ Runs     │            │
│  │ (10s)    │   (10s)     │          │  (0.1s)   │          │            │
│  └──────────┘             │ Aggregate│           └──────────┘            │
│                           │ (30s)    │                                   │
│                           └──────────┘                                   │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

#### Stage 1: Metric recording

Replicas and deployment handles continuously record autoscaling metrics:
- **What**: Number of ongoing requests, both queued and running.
- **Frequency**: Every 10s, configurable through [`metrics_interval_s`](../api/doc/ray.serve.config.AutoscalingConfig.rst).
- **Storage**: Each replica and handle stores metrics locally as a time series.

#### Stage 2: Metric pushing

Periodically, replicas and handles push their metrics to the controller:
- **Frequency**: Every 10s, configurable through `RAY_SERVE_REPLICA_AUTOSCALING_METRIC_PUSH_INTERVAL_S` and `RAY_SERVE_HANDLE_AUTOSCALING_METRIC_PUSH_INTERVAL_S`.
- **Data sent**: Raw time series data. Before sending, replicas and handles clip data points to the [`look_back_period_s`](../api/doc/ray.serve.config.AutoscalingConfig.rst) window, so they send only recent measurements within the window.

#### Stage 3: Metric aggregation

The controller aggregates the raw time series to compute total ongoing requests across all replicas:
- **Input**: Raw time series data from replicas and handles, already clipped to [`look_back_period_s`](../api/doc/ray.serve.config.AutoscalingConfig.rst).
- **Method**: Time-weighted aggregation using the [`aggregation_function`](../api/doc/ray.serve.config.AutoscalingConfig.rst), which is mean, max, or min. Uses an instantaneous merge approach that treats metrics as right-continuous step functions.
- **Output**: A single value representing total ongoing requests.

:::{note}
Earlier releases also supported a "simple mode", selected by `RAY_SERVE_AGGREGATE_METRICS_AT_CONTROLLER`, that summed averages pre-computed at each replica and handle. Ray Serve removed that flag and that mode, so the controller always aggregates raw time series. Deployments that ran with `RAY_SERVE_ENABLE_DIRECT_INGRESS`, which both `RAY_SERVE_THROUGHPUT_OPTIMIZED` and HAProxy turn on, already aggregated at the controller regardless of the flag, so they see no change. If you're upgrading from a release where the flag defaulted to simple mode, expect three consequences:
- [`aggregation_function`](../api/doc/ray.serve.config.AutoscalingConfig.rst) always applies. Simple mode ignored it, so a deployment that set `max` or `min` scaled on a mean under simple mode and scales on a peak or a trough after the upgrade.
- Under the default `"mean"`, steady-state values don't change, but transients do. Simple mode averaged each source's samples without regard to their spacing, whereas the controller weights them by time. Sources that report at different intervals diverge the most.
- Aggregating time series costs the controller more than summing pre-computed averages, and the cost grows with the number of replicas. Watch controller CPU on deployments with high replica counts.
:::

#### Stage 4: Policy execution

The autoscaling policy runs frequently to make scaling decisions. For details on implementing custom scaling logic, see [Custom policy for deployment](#custom-policy-for-deployment). Policy execution has the following characteristics:
- **Frequency**: Every 0.1s, configurable through `RAY_SERVE_CONTROL_LOOP_INTERVAL_S`
- **Input**: [`AutoscalingContext`](../api/doc/ray.serve.config.AutoscalingContext.rst)
- **Output**: A tuple of `(target_replicas, updated_policy_state)`

#### Timing parameter interactions

The timing parameters interact in the following ways:

The recording interval and the push interval relate as follows:
- The push interval is greater than or equal to the recording interval.
- The recording interval, 10s by default, determines the granularity of the data.
- The push interval, 10s by default, determines how fresh the controller's data is.
- With the default values, each push contains one data point: 10s ÷ 10s.

The push interval and the look-back period relate as follows:
- [`look_back_period_s`](../api/doc/ray.serve.config.AutoscalingConfig.rst), 30s by default, should be greater than the push interval, 10s by default.
- If the look-back period is too short, you don't have enough data for stable decisions.
- If the look-back period is too long, autoscaling becomes less responsive.

The push interval and the control loop relate as follows:
- The control loop runs every 0.1s, much faster than metrics arrive every 10s.
- Most control loop iterations reuse existing metrics.
- New scaling decisions happen only when fresh metrics arrive.

The push interval and the upscale and downscale delays relate as follows:
- Delays control when Ray Serve applies a scale up or scale down.
- The metrics push interval controls how quickly Ray Serve receives fresh metrics.
- If the push interval is shorter than the delay, Ray Serve can use multiple metric updates before it scales.
- For example, pushing every 10s with `upscale_delay_s = 20` means up to two new metric updates before scaling.

:::{tip}
Keep the default values unless you have specific needs. If you need faster autoscaling, decrease push intervals first, then adjust delays.
:::

### Environment variables

Several environment variables control autoscaling behavior at a lower level. These variables affect metrics collection and control loop timing.

#### Control loop and timeout settings

* **`RAY_SERVE_CONTROL_LOOP_INTERVAL_S`**: How often the Serve controller runs the autoscaling control loop. Your autoscaling policy function executes at this frequency. The default value of 0.1s means policies run approximately 10 times per second.

* **`RAY_SERVE_RECORD_AUTOSCALING_STATS_TIMEOUT_S`**: Maximum time allowed for the `record_autoscaling_stats()` method to complete in custom metrics collection. The default is 10.0s. If the method exceeds this timeout, the metrics collection fails and Serve logs a warning.

* **`RAY_SERVE_MIN_HANDLE_METRICS_TIMEOUT_S`**: Minimum timeout for handle metrics collection. The default is 10.0s. Serve uses the larger of this value and `2 * `[`metrics_interval_s`](../api/doc/ray.serve.config.AutoscalingConfig.rst) to decide when to drop stale handle metrics.


## Model composition example

To determine the autoscaling configuration for a multi-model application, you need to understand each deployment's scaling requirements. Deployments differ in latency and level of concurrency, so finding the right autoscaling config for a model composition application requires experimentation.

To build some intuition about multi-model autoscaling, this example composes three deployments into one application:
* `HeavyLoad`: A mock 200ms workload with high CPU usage.
* `LightLoad`: A mock 100ms workload with high CPU usage.
* `Driver`: A driver deployment that fans out to the `HeavyLoad` and `LightLoad` deployments and aggregates the two outputs.


### Attempt 1: One `Driver` replica

Start with the following deployment configurations. Because the `Driver` deployment has low CPU usage and only makes asynchronous calls to the downstream deployments, one fixed `Driver` replica is reasonable.

::::{tab-set}

:::{tab-item} Driver

```yaml
- name: Driver
  num_replicas: 1
  max_ongoing_requests: 200
```

:::

:::{tab-item} HeavyLoad

```yaml
- name: HeavyLoad
  max_ongoing_requests: 3
  autoscaling_config:
    target_ongoing_requests: 1
    min_replicas: 0
    initial_replicas: 0
    max_replicas: 200
    upscale_delay_s: 3
    downscale_delay_s: 60
    upscaling_factor: 0.3
    downscaling_factor: 0.3
    metrics_interval_s: 10
    look_back_period_s: 30
```

:::

:::{tab-item} LightLoad

```yaml
- name: LightLoad
  max_ongoing_requests: 3
  autoscaling_config:
    target_ongoing_requests: 1
    min_replicas: 0
    initial_replicas: 0
    max_replicas: 200
    upscale_delay_s: 3
    downscale_delay_s: 60
    upscaling_factor: 0.3
    downscaling_factor: 0.3
    metrics_interval_s: 10
    look_back_period_s: 30
```

:::

:::{tab-item} Application code

```{literalinclude} ../doc_code/autoscale_model_comp_example.py
:language: python
:start-after: __serve_example_begin__
:end-before: __serve_example_end__
```

:::

::::


Running the same Locust load test from the [ResNet workload](resnet-autoscaling-example) generates the following results:

| | |
| ----------------------- | ---------------------- |
| Number of `HeavyLoad` and `LightLoad` replicas | <img src="https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/model_composition_replicas.png" alt="comp" width="600" /> |


As you might expect, autoscaling produces roughly half as many `LightLoad` replicas as `HeavyLoad` replicas. Both deployments receive the same number of requests per second, but `LightLoad` replicas can process twice as many requests per second as `HeavyLoad` replicas, so `LightLoad` should need half as many replicas to handle the same traffic load.

However, the service latency rises from 230 to 400 ms when the number of Locust users increases to 100.

| P50 latency | QPS |
| ------- | --- |
| ![comp_latency](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/model_comp_latency.svg) | ![comp_rps](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/model_comp_rps.svg) |

To adequately serve the Locust traffic, the number of `HeavyLoad` replicas should roughly match the number of Locust users. However, when the number of Locust users increased to 100, the `HeavyLoad` deployment struggled to reach 100 replicas and reached only 65. The per-deployment latencies reveal the root cause. `HeavyLoad` and `LightLoad` latencies stayed steady at 200ms and 100ms, but `Driver` latencies rose from 230 to 400 ms. This result suggests that the high Locust workload might be overwhelming the `Driver` replica and degrading the performance of its asynchronous event loop.


### Attempt 2: Autoscale `Driver`

For this attempt, also set an autoscaling configuration for `Driver`, with `target_ongoing_requests = 20`. The deployment configurations are as follows:

::::{tab-set}

:::{tab-item} Driver

```yaml
- name: Driver
  max_ongoing_requests: 200
  autoscaling_config:
    target_ongoing_requests: 20
    min_replicas: 1
    initial_replicas: 1
    max_replicas: 10
    upscale_delay_s: 3
    downscale_delay_s: 60
    upscaling_factor: 0.3
    downscaling_factor: 0.3
    metrics_interval_s: 10
    look_back_period_s: 30
```

:::

:::{tab-item} HeavyLoad

```yaml
- name: HeavyLoad
  max_ongoing_requests: 3
  autoscaling_config:
    target_ongoing_requests: 1
    min_replicas: 0
    initial_replicas: 0
    max_replicas: 200
    upscale_delay_s: 3
    downscale_delay_s: 60
    upscaling_factor: 0.3
    downscaling_factor: 0.3
    metrics_interval_s: 10
    look_back_period_s: 30
```

:::

:::{tab-item} LightLoad

```yaml
- name: LightLoad
  max_ongoing_requests: 3
  autoscaling_config:
    target_ongoing_requests: 1
    min_replicas: 0
    initial_replicas: 0
    max_replicas: 200
    upscale_delay_s: 3
    downscale_delay_s: 60
    upscaling_factor: 0.3
    downscaling_factor: 0.3
    metrics_interval_s: 10
    look_back_period_s: 30
```

:::
::::

Running the same Locust load test again generates the following results:

| | |
| ------------------------------------ | ------------------- |
| Number of `HeavyLoad` and `LightLoad` replicas | <img src="https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/model_composition_improved_replicas.png" alt="heavy" width="600"/> |
| Number of `Driver` replicas | <img src="https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/model_composition_improved_driver_replicas.png" alt="driver" width="600"/> |

With up to six `Driver` deployments to receive and distribute the incoming requests, the `HeavyLoad` deployment scales up to more than 90 replicas, and `LightLoad` scales up to 47 replicas. This configuration helps keep application latency consistent as the traffic load increases.

| Improved P50 latency | Improved RPS |
| ---------------- | ------------ |
| ![comp_latency](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/model_composition_improved_latency.svg) | ![comp_latency](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/model_comp_improved_rps.svg) |


## Troubleshooting guide

This section covers common autoscaling problems and the settings to try for each.

### Unstable number of autoscaled replicas

If the number of replicas in your deployment keeps oscillating even though the traffic is relatively stable, try the following:

* Set a smaller `upscaling_factor` and `downscaling_factor`. Setting both values below one helps the autoscaler make more conservative upscale and downscale decisions. This effectively smooths out the replica graph and leaves fewer sharp edges.

* Set a `look_back_period_s` value that matches the rest of the autoscaling config. For longer upscale and downscale delays, a longer look-back period can likely help stabilize the replica graph. For shorter delays, a shorter look-back period might be more appropriate. For example, the following replica graphs show how a deployment with `upscale_delay_s = 3` behaves with a longer versus a shorter look-back period.

| `look_back_period_s = 30` | `look_back_period_s = 3` |
| ------------------------------------------------ | ----------------------------------------------- |
| ![look-back-before](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/look_back_period_before.png) | ![look-back-after](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/look_back_period_after.png) |


### High spikes in latency during bursts of traffic

If you expect your application to receive bursty traffic and also want its deployments to scale down during periods of inactivity, you're likely concerned about how quickly a deployment can scale up and respond to a burst. An initial increase in latency during a burst might be unavoidable, but you can try the following to improve latency during bursts of traffic:

* Set a lower `upscale_delay_s`. The autoscaler always waits `upscale_delay_s` seconds before deciding to upscale, so a lower delay makes the autoscaler react more quickly to changes in traffic, especially bursts.

* Set a larger `upscaling_factor`. If `upscaling_factor > 1`, then the autoscaler scales up more aggressively than normal. This setting can make your deployment more sensitive to bursts of traffic.

* Lower the [`metrics_interval_s`](../api/doc/ray.serve.config.AutoscalingConfig.rst). Always set `metrics_interval_s` less than or equal to `upscale_delay_s`. Otherwise, upscaling lags because the autoscaler doesn't receive fresh information often enough. Avoid an extremely low value, such as `0.1`, because it can overload the head node controller and cause instability.

* Set a lower `max_ongoing_requests`. If `max_ongoing_requests` is too high relative to `target_ongoing_requests`, then when traffic increases, Serve might assign most or all of the requests to the existing replicas before the new replicas start. This situation can lead to high latencies during upscale.


### Deployments scaling down too quickly

If your deployments scale down too quickly and you want more conservative downscaling to maximize the availability of your service, try the following:

* Set a longer `downscale_delay_s`. The autoscaler always waits `downscale_delay_s` seconds before deciding to downscale, so a larger value gives your system a longer "grace period" after traffic drops before the autoscaler starts to remove replicas.

* Set a smaller `downscaling_factor`. If `downscaling_factor < 1`, then the autoscaler removes *fewer replicas* than it calculates it should remove to reach the target number of ongoing requests. In other words, the autoscaler makes more conservative downscaling decisions.

| `downscaling_factor = 1` | `downscaling_factor = 0.5` |
| ------------------------------------------------ | ----------------------------------------------- |
| ![downscale-smooth-before](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/downscale_smoothing_factor_before.png) | ![downscale-smooth-after](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/downscale_smoothing_factor_after.png) |


(serve-custom-autoscaling-policies)=
## Custom autoscaling policies

:::{note}
Custom autoscaling policies are a {ref}`beta <api-stability-beta>` feature. The feature is complete and supported for production use. Backwards-incompatible changes are still possible, but Ray keeps them minimal and announces them with a deprecation period.
:::

Ray Serve's built-in, request-driven autoscaling works well for most applications. Use *custom autoscaling policies* when you need more control, such as scaling on external metrics from CloudWatch or Prometheus, anticipating predictable traffic from scheduled batch jobs, or applying business logic that goes beyond queue thresholds.

A custom policy can base its scaling logic on any metrics or rules you choose.

### Custom policy for deployment

A custom autoscaling policy is a Python function you provide that takes an [`AutoscalingContext`](../api/doc/ray.serve.config.AutoscalingContext.rst) and returns a tuple `(target_replicas, policy_state)` for a single deployment.

An `AutoscalingContext` object provides the following information to the custom autoscaling policy:
* **Current state:** Current replica count and deployment metadata.
* **Built-in metrics:** Total requests, queued requests, per-replica counts.
* **Custom metrics:** Values your deployment reports through `record_autoscaling_stats()`, described in [Custom metrics](#custom-metrics).
* **Capacity bounds:** Minimum and maximum replica limits, adjusted for current cluster capacity.
* **Policy state:** A `dict` you can use to persist arbitrary state across control-loop iterations.
* **Timing:** Timestamps of the last scale actions and the current time.

The following example shows a policy that scales up during business hours and evening batch processing, and scales down during off-peak hours:

`autoscaling_policy.py` file:
```{literalinclude} ../doc_code/autoscaling_policy.py
:language: python
:start-after: __begin_scheduled_batch_processing_policy__
:end-before: __end_scheduled_batch_processing_policy__
```

`main.py` file:
```{literalinclude} ../doc_code/scheduled_batch_processing.py
:language: python
:start-after: __serve_example_begin__
:end-before: __serve_example_end__
```

You define a policy per deployment. If you don't provide one, Ray Serve falls back to its built-in request-based policy.

The Serve controller invokes the policy function every `RAY_SERVE_CONTROL_LOOP_INTERVAL_S` seconds, which defaults to 0.1s, so your logic runs against near-real-time state.

Your policy can return an `int` or a `float` for `target_replicas`. If it returns a float, Ray Serve rounds it up to an integer replica count.

:::{warning}
Keep policy functions fast and lightweight. Slow logic can block the Serve controller and degrade cluster responsiveness.
:::


(applying-standard-autoscaling-parameters-to-custom-policies)=
### Apply standard autoscaling parameters to custom policies

Ray Serve automatically applies the following standard autoscaling parameters from your [`AutoscalingConfig`](../api/doc/ray.serve.config.AutoscalingConfig.rst) to custom policies:
- `upscale_delay_s`, `downscale_delay_s`, `downscale_to_zero_delay_s`
- `upscaling_factor`, `downscaling_factor`
- `min_replicas`, `max_replicas`

The following example shows a custom autoscaling policy with standard autoscaling parameters applied.

```{literalinclude} ../doc_code/autoscaling_policy.py
:language: python
:start-after: __begin_apply_autoscaling_config_example__
:end-before: __end_apply_autoscaling_config_example__
```

```{literalinclude} ../doc_code/autoscaling_policy.py
:language: python
:start-after: __begin_apply_autoscaling_config_usage__
:end-before: __end_apply_autoscaling_config_usage__
```

::::{note}
Your policy function should return the raw desired number of replicas, as an `int` or a `float`. Ray Serve applies the `autoscaling_config` delays, factors, and bounds on top of your decision.

Ray Serve returns an integer decision, even when your policy returns a `float`.
::::


### Custom metrics

You can make richer scaling decisions by emitting your own metrics from the deployment. Implement `record_autoscaling_stats()` to return a `dict[str, float]`, and Ray Serve surfaces these values in the [`AutoscalingContext`](../api/doc/ray.serve.config.AutoscalingContext.rst).

The following example shows how a deployment can report its own metrics, such as CPU and memory usage, and how an autoscaling policy can use these metrics to make scaling decisions:

`autoscaling_policy.py` file:
```{literalinclude} ../doc_code/autoscaling_policy.py
:language: python
:start-after: __begin_custom_metrics_autoscaling_policy__
:end-before: __end_custom_metrics_autoscaling_policy__
```

`main.py` file:
```{literalinclude} ../doc_code/custom_metrics_autoscaling.py
:language: python
:start-after: __serve_example_begin__
:end-before: __serve_example_end__
```

:::{note}
The `record_autoscaling_stats()` method can be either synchronous or asynchronous. It must complete within the timeout that `RAY_SERVE_RECORD_AUTOSCALING_STATS_TIMEOUT_S` specifies, which defaults to 10 seconds.
:::

In your policy, access custom metrics through the following attributes:

* `ctx.raw_metrics[metric_name]`: A mapping of replica IDs to lists of raw metric values. The number of data points stored for each replica depends on the sliding window size, [`look_back_period_s`](../api/doc/ray.serve.config.AutoscalingConfig.rst), and the metric recording interval, [`metrics_interval_s`](../api/doc/ray.serve.config.AutoscalingConfig.rst).
* `ctx.aggregated_metrics[metric_name]`: A time-weighted average computed from the raw metric values for each replica.


### Class-based policies

When your policy needs long-running setup, such as polling an external metrics service, maintaining a persistent connection, or running background computation, you can define it as a class instead of a plain function. Pass the class reference through `policy_function` and supply constructor arguments through `policy_kwargs`.

Ray Serve instantiates the class once on the controller when the deployment starts. `__init__` runs one-time setup, and `__call__` runs on every autoscaling tick with the current [`AutoscalingContext`](../api/doc/ray.serve.config.AutoscalingContext.rst).

The following example shows a policy that reads a target replica count from a JSON file in a background loop. In production, you can replace the file read with an HTTP call, a message-queue consumer, or any other async I/O operation:

`class_based_autoscaling_policy.py` file:
```{literalinclude} ../doc_code/class_based_autoscaling_policy.py
:language: python
:start-after: __begin_class_based_autoscaling_policy__
:end-before: __end_class_based_autoscaling_policy__
```

`main.py` file:
```{literalinclude} ../doc_code/class_based_autoscaling.py
:language: python
:start-after: __serve_example_begin__
:end-before: __serve_example_end__
```

:::{note}
The instance lives only on the Serve controller and is never serialized after creation, so it's safe to hold non-picklable state such as `asyncio.Task` objects, open connections, or thread pools. `policy_kwargs` values must be JSON-serializable because they travel through the deployment config.
:::

:::{tip}
If you're using `@task_consumer` deployments for asynchronous inference, Ray Serve provides a built-in `AsyncInferenceAutoscalingPolicy` that scales based on message queue length. See [Asynchronous Inference: Autoscaling](serve-async-inference-autoscaling) for setup and configuration.
:::


### Application-level autoscaling

By default, each deployment in Ray Serve autoscales independently. Some deployments need to scale in a coordinated way, such as deployments that share backend resources, depend on each other, or need load-aware routing. For those, you can define an *application-level autoscaling policy*, which makes scaling decisions for all deployments in an application at the same time.

#### Define an application-level policy

An application-level autoscaling policy is a function that takes a `dict[DeploymentID, AutoscalingContext]`, with one context per deployment, and returns a tuple of `(decisions, policy_state)`. Each context contains metrics and bounds for one deployment, and the policy returns target replica counts for all deployments.

The `policy_state` that an application-level policy returns must be a `Dict[DeploymentID, Dict]`, a dictionary that maps each deployment ID to its own state dictionary. Serve stores this per-deployment state and, on the next control-loop iteration, injects each deployment's state back into that deployment's `AutoscalingContext.policy_state`. The policy can return each deployment's replica count as an `int` or a `float`. Ray Serve rounds a float up to an integer replica count.

Serve doesn't interpret the contents of `policy_state`. You control all the keys in each deployment's state dictionary, except for internal keys that Serve uses when it applies default parameters to custom autoscaling policies. The following example shows a policy that scales deployments based on their relative load, ensuring that downstream deployments have enough capacity for upstream traffic:

`autoscaling_policy.py` file:
```{literalinclude} ../doc_code/autoscaling_policy.py
:language: python
:start-after: __begin_application_level_autoscaling_policy__
:end-before: __end_application_level_autoscaling_policy__
```
The following example shows a stateful application-level policy that persists state between control-loop iterations:

`autoscaling_policy.py` file:
```{literalinclude} ../doc_code/autoscaling_policy.py
:language: python
:start-after: __begin_stateful_application_level_policy__
:end-before: __end_stateful_application_level_policy__
```

#### Configure application-level autoscaling

To use an application-level policy, first define your deployments:

`main.py` file:
```{literalinclude} ../doc_code/application_level_autoscaling.py
:language: python
:start-after: __serve_example_begin__
:end-before: __serve_example_end__
```

Then specify the application-level policy in your application config:

`serve.yaml` file:
```{literalinclude} ../doc_code/application_level_autoscaling.yaml
:language: yaml
:emphasize-lines: 4-5
```

:::{note}
Programmatic configuration of application-level autoscaling policies through `serve.run()` isn't supported yet. A future release will add it.
:::

:::{note}
When you specify both a deployment-level policy and an application-level policy, the application-level policy takes precedence. Ray Serve logs a warning if you configure both.
:::


(applying-standard-autoscaling-parameters-to-application-level-policies)=
#### Apply standard autoscaling parameters to application-level policies
Ray Serve automatically applies standard autoscaling parameters, such as delays, factors, and minimum and maximum bounds, to application-level policies per deployment. These parameters include the following:
- `upscale_delay_s`, `downscale_delay_s`, `downscale_to_zero_delay_s`
- `upscaling_factor`, `downscaling_factor`
- `min_replicas`, `max_replicas`

The following YAML configuration file shows the default parameters applied to the application-level policy.
```{literalinclude} ../doc_code/application_level_autoscaling_with_defaults.yaml
:language: yaml
```
Your application-level policy can return each deployment's desired replica count as an `int` or a `float`. Ray Serve applies the autoscaling config parameters per deployment and returns integer decisions.

### Gotchas and limitations

:::{warning}
Ray Serve can fully support a custom policy as long as it's simple, self-contained Python code that relies only on the standard library. If the policy becomes more complex, such as depending on other custom modules or packages, bundle those modules into the Docker image or environment. Ray Serve uses `cloudpickle` to serialize custom policies and doesn't vendor transitive dependencies. If your policy inherits from a superclass in another module or imports custom packages, those modules and packages must exist in the target environment. Environment parity also matters. Differences in Python version, `cloudpickle` version, or library versions can affect deserialization.
:::

#### Alternatives for complex policies

When your custom autoscaling policy has complex dependencies or you want better control over versioning and deployment, you have two alternatives:

- **Contribute to Ray Serve**: If your policy is general-purpose and might benefit others, consider contributing it to Ray Serve as a built-in policy by opening a feature request or pull request on the [Ray GitHub repository](https://github.com/ray-project/ray/issues). The recommended location for the implementation is `python/ray/serve/autoscaling_policy.py`.
- **Ensure dependencies in your environment**: Install the external dependencies in your Docker image or environment.


(serve-external-scale-api)=

### External scaling API

:::{warning}
This API is in alpha and might change before becoming stable.
:::

The external scaling API gives you programmatic control over the number of replicas for any deployment in your Ray Serve application. Unlike Ray Serve's built-in autoscaling, which scales based on queue depth and ongoing requests, this API scales deployments based on any external criteria you define.

#### Example: Predictive scaling

This example shows how to implement predictive scaling based on historical patterns or forecasts. You can scale up ahead of anticipated traffic spikes by running an external script that adjusts replica counts based on the time of day.

##### Define the deployment

The following example creates a text-processing deployment that you can scale externally. Save this code to a file named `external_scaler_predictive.py`:

```{literalinclude} ../doc_code/external_scaler_predictive.py
:language: python
:start-after: __serve_example_begin__
:end-before: __serve_example_end__
```

##### Configure external scaling

Before using the external scaling API, enable it in your application configuration by setting `external_scaler_enabled: true`. Save this configuration to a file named `external_scaler_config.yaml`:

```{literalinclude} ../doc_code/external_scaler_config.yaml
:language: yaml
:start-after: __external_scaler_config_begin__
:end-before: __external_scaler_config_end__
```

:::{warning}
External scaling and built-in autoscaling are mutually exclusive. You can't use both for the same application. If you set `external_scaler_enabled: true`, don't configure `autoscaling_config` on any deployment in that application. Using both results in an error.
:::

##### Implement the scaling logic

The following script implements predictive scaling based on time of day and historical traffic patterns. Save this script to a file named `external_scaler_predictive_client.py`:

```{literalinclude} ../doc_code/external_scaler_predictive_client.py
:language: python
:start-after: __client_script_begin__
:end-before: __client_script_end__
```

The script calls the following external scaling API endpoint to scale deployments:
- **API endpoint**: `POST http://localhost:8265/api/v1/applications/{application_name}/deployments/{deployment_name}/scale`.
- **Request body**: `{"target_num_replicas": <number>}`, which must conform to the [`ScaleDeploymentRequest`](../api/doc/ray.serve.schema.ScaleDeploymentRequest.rst) schema.

The scaling client continuously adjusts the number of replicas based on the time of day:
- Business hours, 9 AM to 5 PM: 10 replicas
- Off-peak hours: 3 replicas

##### Run the example

Follow these steps to run the complete example:

1. Start the Ray Serve application with the configuration:

   ```bash
   serve run external_scaler_config.yaml
   ```

1. Run the predictive scaling client in a separate terminal:

   ```bash
   python external_scaler_predictive_client.py
   ```

The client adjusts replica counts automatically based on the time of day. You can monitor the scaling behavior in the Ray dashboard or by checking the application logs.

#### Important considerations

Keep the following behaviors in mind to build reliable scaling logic with the external scaler:

- **Idempotent API calls**: The scaling API is idempotent. You can safely call it multiple times with the same `target_num_replicas` value without side effects, so you can run your scaling logic on a schedule or in response to repeated metric updates.

- **Interaction with serve deploy**: When you upgrade your service with `serve deploy`, the number of replicas you set through the external scaler API stays intact. This behavior matches Ray Serve's built-in autoscaler, where deployment updates don't reset replica counts.

- **Query current replica count**: You can get the current number of replicas for any deployment by querying the GET `/applications` API:

  ```bash
  curl -X GET http://localhost:8265/api/serve/applications/ \
  ```

  The response follows the [`ServeInstanceDetails`](../api/doc/ray.serve.schema.ServeInstanceDetails.rst) schema, which includes an `applications` field that contains a dictionary keyed by application name. Each application entry includes details about all its deployments, including current replica counts. Use this information to make informed scaling decisions. For example, you might scale up gradually by adding a percentage of existing replicas rather than jumping to a fixed number.

- **Initial replica count**: When you deploy an application for the first time, Ray Serve creates the number of replicas that the `num_replicas` field of your deployment configuration specifies. The external scaler can then adjust this count dynamically based on your scaling logic.
