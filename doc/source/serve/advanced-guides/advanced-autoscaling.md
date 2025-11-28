(serve-advanced-autoscaling)=

# Advanced Ray Serve Autoscaling

This guide goes over more advanced autoscaling parameters in
[autoscaling_config](../api/doc/ray.serve.config.AutoscalingConfig.rst) and an
advanced model composition example.


(serve-autoscaling-config-parameters)=
## Autoscaling config parameters

In this section, we go into more detail about Serve autoscaling concepts as well
as how to set your autoscaling config.

### [Required] Define the steady state of your system

To define what the steady state of your deployments should be, set values for
`target_ongoing_requests` and `max_ongoing_requests`.

#### **[`target_ongoing_requests`](../api/doc/ray.serve.config.AutoscalingConfig.target_ongoing_requests.rst) [default=2]**
:::{note}
The default for `target_ongoing_requests` changed from 1.0 to 2.0 in Ray 2.32.0.
You can continue to set it manually to override the default.
:::

Serve scales the number of replicas for a deployment up or down based on the
average number of ongoing requests per replica. Specifically, Serve compares the
*actual* number of ongoing requests per replica with the target value you set in
the autoscaling config and makes upscale or downscale decisions from that. Set
the target value with `target_ongoing_requests`, and Serve attempts to ensure
that each replica has roughly that number of requests being processed and
waiting in the queue. 

Always load test your workloads. For example, if the use case is latency
sensitive, you can lower the `target_ongoing_requests` number to maintain high
performance. Benchmark your application code and set this number based on an
end-to-end latency objective.

:::{note}
As an example, suppose you have two replicas of a synchronous deployment that
has 100ms latency, serving a traffic load of 30 QPS. Then Serve assigns requests
to replicas faster than the replicas can finish processing them; more and more
requests queue up at the replica (these requests are "ongoing requests") as time
progresses, and then the average number of ongoing requests at each replica
steadily increases. Latency also increases because new requests have to wait for
old requests to finish processing. If you set `target_ongoing_requests = 1`,
Serve detects a higher than desired number of ongoing requests per replica, and
adds more replicas. At 3 replicas, your system would be able to process 30 QPS
with 1 ongoing request per replica on average.
:::

#### **`max_ongoing_requests` [default=5]**
:::{note}
The default for `max_ongoing_requests` changed from 100 to 5 in Ray 2.32.0.
You can continue to set it manually to override the default.
:::

There is also a maximum queue limit that proxies respect when assigning requests
to replicas. Define the limit with `max_ongoing_requests`. Set
`max_ongoing_requests` to ~20 to 50% higher than `target_ongoing_requests`.

- Setting it too low can throttle throughput. Instead of being forwarded to
replicas for concurrent execution, requests will tend to queue up at the proxy,
waiting for replicas to finish processing existing requests.

:::{note}
`max_ongoing_requests` should be tuned higher especially for lightweight
requests, else the overall throughput will be impacted.
:::

- Setting it too high can lead to imbalanced routing. Concretely, this can lead
to very high tail latencies during upscale, because when the autoscaler is
scaling a deployment up due to a traffic spike, most or all of the requests
might be assigned to the existing replicas before the new replicas are started.


### [Required] Define upper and lower autoscaling limits

To use autoscaling, you need to define the minimum and maximum number of
resources allowed for your system.

* **[`min_replicas`](../api/doc/ray.serve.config.AutoscalingConfig.min_replicas.rst) [default=1]**: This is the minimum number of replicas for the
deployment. If you want to ensure your system can deal with a certain level of
traffic at all times, set `min_replicas` to a positive number. On the other
hand, if you anticipate periods of no traffic and want to scale to zero to save
cost, set `min_replicas = 0`. Note that setting `min_replicas = 0` causes higher
tail latencies; when you start sending traffic, the deployment scales up, and
there will be a cold start time as Serve waits for replicas to be started to
serve the request.

* **[`max_replicas`](../api/doc/ray.serve.config.AutoscalingConfig.max_replicas.rst) [default=1]**: This is the maximum number of replicas for the
deployment. This should be greater than `min_replicas`. Ray Serve Autoscaling
relies on the Ray Autoscaler to scale up more nodes when the currently available
cluster resources (CPUs, GPUs, etc.) are not enough to support more replicas.

* **[`initial_replicas`](../api/doc/ray.serve.config.AutoscalingConfig.initial_replicas.rst)**: This is the number of replicas that are started
initially for the deployment. This defaults to the value for `min_replicas`.


### [Optional] Define how the system reacts to changing traffic

Given a steady stream of traffic and appropriately configured `min_replicas` and
`max_replicas`, the steady state of your system is essentially fixed for a
chosen configuration value for `target_ongoing_requests`. Before reaching steady
state, however, your system is reacting to traffic shifts. How you want your
system to react to changes in traffic determines how you want to set the
remaining autoscaling configurations.

* **[`upscale_delay_s`](../api/doc/ray.serve.config.AutoscalingConfig.upscale_delay_s.rst) [default=30s]**: This defines how long Serve waits before
scaling up the number of replicas in your deployment. In other words, this
parameter controls the frequency of upscale decisions. If the replicas are
*consistently* serving more requests than desired for an `upscale_delay_s`
number of seconds, then Serve scales up the number of replicas based on
aggregated ongoing requests metrics. For example, if your service is likely to
experience bursts of traffic, you can lower `upscale_delay_s` so that your
application can react quickly to increases in traffic.

Ray Serve allows you to use different delays for different downscaling scenarios,
providing more granular control over when replicas are removed. This is particularly
useful when you want different behavior for scaling down to zero versus scaling
down to a non-zero number of replicas.

* **[`downscale_delay_s`](../api/doc/ray.serve.config.AutoscalingConfig.downscale_delay_s.rst) [default=600s]**: This defines how long Serve waits before
scaling down the number of replicas in your deployment. If the replicas are
*consistently* serving fewer requests than desired for a `downscale_delay_s`
number of seconds, Serve scales down the number of replicas based on
aggregated ongoing requests metrics. This delay applies to all downscaling
decisions except for the optional 1→0 transition (see below). For example, if
your application initializes slowly, you can increase `downscale_delay_s` to
make downscaling happen more infrequently and avoid reinitialization costs when
the application needs to upscale again.

* **[`downscale_to_zero_delay_s`](../api/doc/ray.serve.config.AutoscalingConfig.downscale_to_zero_delay_s.rst) [Optional]**: This defines how long Serve waits
before scaling from one replica down to zero (only applies when `min_replicas = 0`).
If not specified, the 1→0 transition uses the `downscale_delay_s` value. This is
useful when you want more conservative scale-to-zero behavior. For example, you
might set `downscale_delay_s = 300` for regular downscaling but
`downscale_to_zero_delay_s = 1800` to wait 30 minutes before scaling to zero,
avoiding cold starts for brief periods of inactivity.

* **[`upscale_smoothing_factor`](../api/doc/ray.serve.config.AutoscalingConfig.upscale_smoothing_factor.rst) [default_value=1.0] (DEPRECATED)**: This parameter
is renamed to `upscaling_factor`. `upscale_smoothing_factor` will be removed in
a future release.

* **[`downscale_smoothing_factor`](../api/doc/ray.serve.config.AutoscalingConfig.downscale_smoothing_factor.rst) [default_value=1.0] (DEPRECATED)**: This
parameter is renamed to `downscaling_factor`. `downscale_smoothing_factor` will
be removed in a future release.

* **[`upscaling_factor`](../api/doc/ray.serve.config.AutoscalingConfig.upscaling_factor.rst) [default_value=1.0]**: The multiplicative factor to amplify
or moderate each upscaling decision. For example, when the application has high
traffic volume in a short period of time, you can increase `upscaling_factor` to
scale up the resource quickly. This parameter is like a "gain" factor to amplify
the response of the autoscaling algorithm.

* **[`downscaling_factor`](../api/doc/ray.serve.config.AutoscalingConfig.downscaling_factor.rst) [default_value=1.0]**: The multiplicative factor to
amplify or moderate each downscaling decision. For example, if you want your
application to be less sensitive to drops in traffic and scale down more
conservatively, you can decrease `downscaling_factor` to slow down the pace of
downscaling.

* **[`metrics_interval_s`](../api/doc/ray.serve.config.AutoscalingConfig.metrics_interval_s.rst) [default_value=10]**: In future this deployment level
config will be removed in favor of cross-application level global config.
  
This controls how often each replica and handle sends reports on current ongoing
requests to the autoscaler. Note that the autoscaler can't make new decisions if
it doesn't receive updated metrics, so you most likely want to set these values to
be less than or equal to the upscale and downscale delay values. For instance, if
you set `upscale_delay_s = 3`, but keep the push interval at 10s, the autoscaler
only upscales roughly every 10 seconds.

* **[`look_back_period_s`](../api/doc/ray.serve.config.AutoscalingConfig.look_back_period_s.rst) [default_value=30]**: This is the window over which the
average number of ongoing requests per replica is calculated.

* **[`aggregation_function`](../api/doc/ray.serve.config.AutoscalingConfig.aggregation_function.rst) [default_value="mean"]**: This controls how metrics are
aggregated over the `look_back_period_s` time window. The aggregation function
determines how Ray Serve combines multiple metric measurements into a single
value for autoscaling decisions. Supported values:
  - `"mean"` (default): Uses time-weighted average of metrics. This provides
  smooth scaling behavior that responds to sustained traffic patterns.
  - `"max"`: Uses the maximum metric value observed. This makes autoscaling more
  sensitive to spikes, scaling up quickly when any replica experiences high load.
  - `"min"`: Uses the minimum metric value observed. This results in more
  conservative scaling behavior.

For most workloads, the default `"mean"` aggregation provides the best balance.
Use `"max"` if you need to react quickly to traffic spikes, or `"min"` if you
prefer conservative scaling that avoids rapid fluctuations.

### How autoscaling metrics work

Understanding how metrics flow through the autoscaling system helps you configure
the parameters effectively. The metrics pipeline involves several stages, each
with its own timing parameters:

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
- **What**: Number of ongoing requests (queued + running)
- **Frequency**: Every 10s (configurable via [`metrics_interval_s`](../api/doc/ray.serve.config.AutoscalingConfig.metrics_interval_s.rst))
- **Storage**: Metrics are stored locally as a timeseries

#### Stage 2: Metric pushing

Periodically, replicas and handles push their metrics to the controller:
- **Frequency**: Every 10s (configurable via `RAY_SERVE_REPLICA_AUTOSCALING_METRIC_PUSH_INTERVAL_S` and `RAY_SERVE_HANDLE_AUTOSCALING_METRIC_PUSH_INTERVAL_S`)
- **Data sent**: Both raw timeseries data and pre-aggregated metrics
  - **Raw timeseries**: Data points are clipped to the [`look_back_period_s`](../api/doc/ray.serve.config.AutoscalingConfig.look_back_period_s.rst) window before sending (only recent measurements within the window are sent)
  - **Pre-aggregated metrics**: A simple average computed over the [`look_back_period_s`](../api/doc/ray.serve.config.AutoscalingConfig.look_back_period_s.rst) window at the replica/handle
- **Controller usage**: The controller decides which data to use based on the `RAY_SERVE_AGGREGATE_METRICS_AT_CONTROLLER` setting (see Stage 3 below)

#### Stage 3: Metric aggregation

The controller aggregates metrics to compute total ongoing requests across all replicas.
Ray Serve supports two aggregation modes (controlled by `RAY_SERVE_AGGREGATE_METRICS_AT_CONTROLLER`):

**Simple mode (default - `RAY_SERVE_AGGREGATE_METRICS_AT_CONTROLLER=0`):**
- **Input**: Pre-aggregated simple averages from replicas/handles (already clipped to [`look_back_period_s`](../api/doc/ray.serve.config.AutoscalingConfig.look_back_period_s.rst))
- **Method**: Sums the pre-aggregated values from all sources. Each component computes a simple average (arithmetic mean) before sending.
- **Output**: Single value representing total ongoing requests
- **Characteristics**: Lightweight and works well for most workloads. However, because it uses simple averages rather than time-weighted averages, it can be less accurate when replicas have different metric reporting intervals or when metrics arrive at different times.

**Aggregate mode (experimental - `RAY_SERVE_AGGREGATE_METRICS_AT_CONTROLLER=1`):**
- **Input**: Raw timeseries data from replicas/handles (already clipped to [`look_back_period_s`](../api/doc/ray.serve.config.AutoscalingConfig.look_back_period_s.rst))
- **Method**: Time-weighted aggregation using the [`aggregation_function`](../api/doc/ray.serve.config.AutoscalingConfig.aggregation_function.rst) (mean, max, or min). Uses an instantaneous merge approach that treats metrics as right-continuous step functions.
- **Output**: Single value representing total ongoing requests
- **Characteristics**: Provides more mathematically accurate aggregation, especially when replicas report metrics at different intervals or you need precise time-weighted averages. The trade-off is increased controller overhead.

:::{note}
The [`aggregation_function`](../api/doc/ray.serve.config.AutoscalingConfig.aggregation_function.rst) parameter only applies in aggregate mode. In simple mode, the aggregation is always a sum of the pre-computed simple averages.
:::

:::{note}
The long-term plan is to deprecate simple mode in favor of aggregate mode. Aggregate mode provides more accurate metrics aggregation and will become the default in a future release. Consider testing aggregate mode(`RAY_SERVE_AGGREGATE_METRICS_AT_CONTROLLER=1`) in your deployments to prepare for this transition.
:::

#### Stage 4: Policy execution

The autoscaling policy runs frequently to make scaling decisions, see [Custom policy for deployment](#custom-policy-for-deployment) for details on implementing custom scaling logic:
- **Frequency**: Every 0.1s (configurable via `RAY_SERVE_CONTROL_LOOP_INTERVAL_S`)
- **Input**: [`AutoscalingContext`](../api/doc/ray.serve.config.AutoscalingContext.rst)
- **Output**: Tuple of `(target_replicas, updated_policy_state)`

#### Timing parameter interactions

The timing parameters interact in important ways:

**Recording vs pushing intervals:**
- Push interval ≥ Recording interval
- Recording interval (10s) determines granularity of data
- Push interval (10s) determines how fresh the controller's data is
- With default values: Each push contains 1 data points (10s ÷ 10s)

**Push interval vs look-back period:**
- [`look_back_period_s`](../api/doc/ray.serve.config.AutoscalingConfig.look_back_period_s.rst) (30s) should be ≥ push interval (10s)
- If look-back is too short, you won't have enough data for stable decisions
- If look-back is too long, autoscaling becomes less responsive

**Push interval vs control loop:**
- Control loop (0.1s) runs much faster than metrics arrive (10s)
- Most control loop iterations reuse existing metrics
- New scaling decisions only happen when fresh metrics arrive

**Push interval vs upscale/downscale delays:**
- Delays (30s/600s) should be ≥ push interval (10s)
- Generally the delay should be set to some multiples of push interval, so the autoscaler only reacts after 
  multiple consecutive metric breaches—this filters out short-lived spikes and prevents noisy, oscillating scale-ups.
- Example: `upscale_delay_s = 5` but push interval = 10s means actual delay ≈ 10s

**Recommendation:** Keep default values unless you have specific needs. If you
need faster autoscaling, decrease push intervals first, then adjust delays.

### Environment variables

Several environment variables control autoscaling behavior at a lower level. These
variables affect metrics collection and the control loop timing:

#### Control loop and timeout settings

* **`RAY_SERVE_CONTROL_LOOP_INTERVAL_S`** (default: 0.1s): How often the Ray
Serve controller runs the autoscaling control loop. Your autoscaling policy
function executes at this frequency. The default value of 0.1s means policies
run approximately 10 times per second.

* **`RAY_SERVE_RECORD_AUTOSCALING_STATS_TIMEOUT_S`** (default: 10.0s): Maximum
time allowed for the `record_autoscaling_stats()` method to complete in custom
metrics collection. If this timeout is exceeded, the metrics collection fails
and a warning is logged.

* **`RAY_SERVE_MIN_HANDLE_METRICS_TIMEOUT_S`** (default: 10.0s): Minimum timeout
for handle metrics collection. The system uses the maximum of this value and
`2 * `[`metrics_interval_s`](../api/doc/ray.serve.config.AutoscalingConfig.metrics_interval_s.rst) to determine when to drop stale handle metrics.

#### Advanced feature flags

* **`RAY_SERVE_AGGREGATE_METRICS_AT_CONTROLLER`** (default: false): Enables an
experimental metrics aggregation mode where the controller aggregates raw
timeseries data instead of using pre-aggregated metrics. This mode provides more
accurate time-weighted averages but may increase controller overhead. See Stage 3
in "How autoscaling metrics work" for details.


## Model composition example

Determining the autoscaling configuration for a multi-model application requires
understanding each deployment's scaling requirements. Every deployment has a
different latency and differing levels of concurrency. As a result, finding the
right autoscaling config for a model-composition application requires
experimentation.

This example is a simple application with three deployments composed together to
build some intuition about multi-model autoscaling. Assume these deployments:
* `HeavyLoad`: A mock 200ms workload with high CPU usage.
* `LightLoad`: A mock 100ms workload with high CPU usage.
* `Driver`: A driver deployment that fans out to the `HeavyLoad` and `LightLoad`
deployments and aggregates the two outputs.


### Attempt 1: One `Driver` replica

First consider the following deployment configurations. Because the driver
deployment has low CPU usage and is only asynchronously making calls to the
downstream deployments, allocating one fixed `Driver` replica is reasonable.

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
    metrics_interval_s: 2
    look_back_period_s: 10
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
    metrics_interval_s: 2
    look_back_period_s: 10
```

:::

:::{tab-item} Application Code

```{literalinclude} ../doc_code/autoscale_model_comp_example.py
:language: python
:start-after: __serve_example_begin__
:end-before: __serve_example_end__
```

:::

::::


Running the same Locust load test from the
[Resnet workload](resnet-autoscaling-example) generates the following results:

| | |
| ----------------------- | ---------------------- |
| HeavyLoad and LightLoad Number Replicas | <img src="https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/model_composition_replicas.png" alt="comp" width="600" /> |


As you might expect, the number of autoscaled `LightLoad` replicas is roughly
half that of autoscaled `HeavyLoad` replicas. Although the same number of
requests per second are sent to both deployments, `LightLoad` replicas can
process twice as many requests per second as `HeavyLoad` replicas can, so the
deployment should need half as many replicas to handle the same traffic load.

Unfortunately, the service latency rises to from 230 to 400 ms when the number
of Locust users increases to 100.

| P50 Latency | QPS |
| ------- | --- |
| ![comp_latency](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/model_comp_latency.svg) | ![comp_rps](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/model_comp_rps.svg) |

Note that the number of `HeavyLoad` replicas should roughly match the number of
Locust users to adequately serve the Locust traffic. However, when the number of
Locust users increased to 100, the `HeavyLoad` deployment struggled to reach 100
replicas, and instead only reached 65 replicas. The per-deployment latencies
reveal the root cause. While `HeavyLoad` and `LightLoad` latencies stayed steady
at 200ms and 100ms, `Driver` latencies rose from 230 to 400 ms. This suggests
that the high Locust workload may be overwhelming the `Driver` replica and
impacting its asynchronous event loop's performance.


### Attempt 2: Autoscale `Driver`

For this attempt, set an autoscaling configuration for `Driver` as well, with
the setting `target_ongoing_requests = 20`. Now the deployment configurations
are as follows:

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
    metrics_interval_s: 2
    look_back_period_s: 10
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
    metrics_interval_s: 2
    look_back_period_s: 10
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
    metrics_interval_s: 2
    look_back_period_s: 10
```

:::
::::

Running the same Locust load test again generates the following results:

| | |
| ------------------------------------ | ------------------- |
| HeavyLoad and LightLoad Number Replicas | <img src="https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/model_composition_improved_replicas.png" alt="heavy" width="600"/> |
| Driver Number Replicas | <img src="https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/model_composition_improved_driver_replicas.png" alt="driver" width="600"/> |

With up to 6 `Driver` deployments to receive and distribute the incoming
requests, the `HeavyLoad` deployment successfully scales up to 90+ replicas, and
`LightLoad` up to 47 replicas. This configuration helps the application latency
stay consistent as the traffic load increases.

| Improved P50 Latency | Improved RPS |
| ---------------- | ------------ |
| ![comp_latency](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/model_composition_improved_latency.svg) | ![comp_latency](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/model_comp_improved_rps.svg) |


## Troubleshooting guide


### Unstable number of autoscaled replicas

If the number of replicas in your deployment keeps oscillating even though the
traffic is relatively stable, try the following:

* Set a smaller `upscaling_factor` and `downscaling_factor`. Setting both values
smaller than one helps the autoscaler make more conservative upscale and
downscale decisions. It effectively smooths out the replicas graph, and there
will be less "sharp edges".

* Set a `look_back_period_s` value that matches the rest of the autoscaling
config. For longer upscale and downscale delay values, a longer look back period
can likely help stabilize the replica graph, but for shorter upscale and
downscale delay values, a shorter look back period may be more appropriate. For
instance, the following replica graphs show how a deployment with
`upscale_delay_s = 3` works with a longer vs shorter look back period.

| `look_back_period_s = 30` | `look_back_period_s = 3` |
| ------------------------------------------------ | ----------------------------------------------- |
| ![look-back-before](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/look_back_period_before.png) | ![look-back-after](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/look_back_period_after.png) |


### High spikes in latency during bursts of traffic

If you expect your application to receive bursty traffic, and at the same time
want the deployments to scale down in periods of inactivity, you are likely
concerned about how quickly the deployment can scale up and respond to bursts of
traffic. While an increase in latency initially during a burst in traffic may be
unavoidable, you can try the following to improve latency during bursts of
traffic.

* Set a lower `upscale_delay_s`. The autoscaler always waits `upscale_delay_s`
seconds before making a decision to upscale, so lowering this delay allows the
autoscaler to react more quickly to changes, especially bursts, of traffic.

* Set a larger `upscaling_factor`. If `upscaling_factor > 1`, then the
autoscaler scales up more aggressively than normal. This setting can allow your
deployment to be more sensitive to bursts of traffic.

* Lower the [`metrics_interval_s`](../api/doc/ray.serve.config.AutoscalingConfig.metrics_interval_s.rst).
Always set [`metrics_interval_s`](../api/doc/ray.serve.config.AutoscalingConfig.metrics_interval_s.rst) to be less than
or equal to `upscale_delay_s`, otherwise upscaling is delayed because the
autoscaler doesn't receive fresh information often enough.

* Set a lower `max_ongoing_requests`. If `max_ongoing_requests` is too high
relative to `target_ongoing_requests`, then when traffic increases, Serve might
assign most or all of the requests to the existing replicas before the new
replicas are started. This setting can lead to very high latencies during
upscale.


### Deployments scaling down too quickly

You may observe that deployments are scaling down too quickly. Instead, you may
want the downscaling to be much more conservative to maximize the availability
of your service.

* Set a longer `downscale_delay_s`. The autoscaler always waits
`downscale_delay_s` seconds before making a decision to downscale, so by
increasing this number, your system has a longer "grace period" after traffic
drops before the autoscaler starts to remove replicas.

* Set a smaller `downscaling_factor`. If `downscaling_factor < 1`, then the
autoscaler removes *less replicas* than what it thinks it should remove to
achieve the target number of ongoing requests. In other words, the autoscaler
makes more conservative downscaling decisions.

| `downscaling_factor = 1` | `downscaling_factor = 0.5` |
| ------------------------------------------------ | ----------------------------------------------- |
| ![downscale-smooth-before](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/downscale_smoothing_factor_before.png) | ![downscale-smooth-after](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/downscale_smoothing_factor_after.png) |


(serve-custom-autoscaling-policies)=
## Custom autoscaling policies

:::{warning}
Custom autoscaling policies are experimental and may change in future releases.
:::

Ray Serve’s built-in, request-driven autoscaling works well for most apps. Use **custom autoscaling policies** when you need more control—e.g., scaling on external metrics (CloudWatch, Prometheus), anticipating predictable traffic (scheduled batch jobs), or applying business logic that goes beyond queue thresholds.

Custom policies let you implement scaling logic based on any metrics or rules you choose.

### Custom policy for deployment

A custom autoscaling policy is a user-provided Python function that takes an [`AutoscalingContext`](../api/doc/ray.serve.config.AutoscalingContext.rst) and returns a tuple `(target_replicas, policy_state)` for a single Deployment.

* **Current state:** Current replica count and deployment metadata.
* **Built-in metrics:** Total requests, queued requests, per-replica counts.
* **Custom metrics:** Values your deployment reports via `record_autoscaling_stats()`. (See below.)
* **Capacity bounds:** `min` / `max` replica limits adjusted for current cluster capacity.
* **Policy state:** A `dict` you can use to persist arbitrary state across control-loop iterations.
* **Timing:** Timestamps of the last scale actions and “now”.

The following example showcases a policy that scales up during business hours and evening batch processing, and scales down during off-peak hours:

```{literalinclude} ../doc_code/autoscaling_policy.py
:language: python
:start-after: __begin_scheduled_batch_processing_policy__
:end-before: __end_scheduled_batch_processing_policy__
```

```{literalinclude} ../doc_code/scheduled_batch_processing.py
:language: python
:start-after: __serve_example_begin__
:end-before: __serve_example_end__
```

Policies are defined **per deployment**. If you don’t provide one, Ray Serve falls back to its built-in request-based policy.

The policy function is invoked by the Ray Serve controller every `RAY_SERVE_CONTROL_LOOP_INTERVAL_S` seconds (default **0.1s**), so your logic runs against near-real-time state.

:::{warning}
Keep policy functions **fast and lightweight**. Slow logic can block the Serve controller and degrade cluster responsiveness.
:::


### Custom metrics

You can make richer decisions by emitting your own metrics from the deployment. Implement `record_autoscaling_stats()` to return a `dict[str, float]`. Ray Serve will surface these values in the [`AutoscalingContext`](../api/doc/ray.serve.config.AutoscalingContext.rst).

This example demonstrates how deployments can provide their own metrics (CPU usage, memory usage) and how autoscaling policies can use these metrics to make scaling decisions:

```{literalinclude} ../doc_code/autoscaling_policy.py
:language: python
:start-after: __begin_custom_metrics_autoscaling_policy__
:end-before: __end_custom_metrics_autoscaling_policy__
```

```{literalinclude} ../doc_code/custom_metrics_autoscaling.py
:language: python
:start-after: __serve_example_begin__
:end-before: __serve_example_end__
```

:::{note}
The `record_autoscaling_stats()` method can be either synchronous or asynchronous. It must complete within the timeout specified by `RAY_SERVE_RECORD_AUTOSCALING_STATS_TIMEOUT_S` (default 10 seconds).
:::

In your policy, access custom metrics via:

* **`ctx.raw_metrics[metric_name]`** — A mapping of replica IDs to lists of raw metric values.
  The number of data points stored for each replica depends on the [`look_back_period_s`](../api/doc/ray.serve.config.AutoscalingConfig.look_back_period_s.rst) (the sliding window size) and [`metrics_interval_s`](../api/doc/ray.serve.config.AutoscalingConfig.metrics_interval_s.rst) (the metric recording interval).
* **`ctx.aggregated_metrics[metric_name]`** — A time-weighted average computed from the raw metric values for each replica.


### Application level autoscaling

By default, each deployment in Ray Serve autoscales independently. When you have multiple deployments that need to scale in a coordinated way—such as deployments that share backend resources, have dependencies on each other, or need load-aware routing—you can define an **application-level autoscaling policy**. This policy makes scaling decisions for all deployments within an application simultaneously.

#### Define an application level policy

An application-level autoscaling policy is a function that takes a Dict[DeploymentID, [`AutoscalingContext`](../api/doc/ray.serve.config.AutoscalingContext.rst)] objects (one per deployment) and returns a tuple of `(decisions, policy_state)`. Each context contains metrics and bounds for one deployment, and the policy returns target replica counts for all deployments.

The following example shows a policy that scales deployments based on their relative load, ensuring that downstream deployments have enough capacity for upstream traffic:

```{literalinclude} ../doc_code/autoscaling_policy.py
:language: python
:start-after: __begin_application_level_autoscaling_policy__
:end-before: __end_application_level_autoscaling_policy__
```

#### Configure application level autoscaling

To use an application-level policy, you need to define your deployments:

```{literalinclude} ../doc_code/application_level_autoscaling.py
:language: python
:start-after: __serve_example_begin__
:end-before: __serve_example_end__
```

Then specify the application-level policy in your application config:

```{literalinclude} ../doc_code/application_level_autoscaling.yaml
:language: yaml
:emphasize-lines: 4-5
```

:::{note}
Programmatic configuration of application-level autoscaling policies through `serve.run()` will be supported in a future release.
:::

:::{note}
When you specify both a deployment-level policy and an application-level policy, the application-level policy takes precedence. Ray Serve logs a warning if you configure both.
:::

:::{warning}
### Gotchas and limitations

When you provide a custom policy, Ray Serve can fully support it as long as it's simple, self-contained Python code that relies only on the standard library. Once the policy becomes more complex, such as depending on other custom modules or packages, you need to bundle those modules into the Docker image or environment. This is because Ray Serve uses `cloudpickle` to serialize custom policies and it doesn't vendor transitive dependencies—if your policy inherits from a superclass in another module or imports custom packages, those must exist in the target environment. Additionally, environment parity matters: differences in Python version, `cloudpickle` version, or library versions can affect deserialization.

#### Alternatives for complex policies

When your custom autoscaling policy has complex dependencies or you want better control over versioning and deployment, you have several alternatives:

- **Contribute to Ray Serve**: If your policy is general-purpose and might benefit others, consider contributing it to Ray Serve as a built-in policy by opening a feature request or pull request on the [Ray GitHub repository](https://github.com/ray-project/ray/issues). The recommended location for the implementation is `python/ray/serve/autoscaling_policy.py`.
- **Ensure dependencies in your environment**: Make sure that the external dependencies are installed in your Docker image or environment.
:::


(serve-external-scale-api)=

### External scaling API

:::{warning}
This API is in alpha and may change before becoming stable.
:::

The external scaling API provides programmatic control over the number of replicas for any deployment in your Ray Serve application. Unlike Ray Serve's built-in autoscaling, which scales based on queue depth and ongoing requests, this API allows you to scale based on any external criteria you define.

#### Example: Predictive scaling

This example shows how to implement predictive scaling based on historical patterns or forecasts. You can preemptively scale up before anticipated traffic spikes by running an external script that adjusts replica counts based on time of day.

##### Define the deployment

The following example creates a simple text processing deployment that you can scale externally. Save this code to a file named `external_scaler_predictive.py`:

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
External scaling and built-in autoscaling are mutually exclusive. You can't use both for the same application. If you set `external_scaler_enabled: true`, you **must not** configure `autoscaling_config` on any deployment in that application. Attempting to use both results in an error.
:::

##### Implement the scaling logic

The following script implements predictive scaling based on time of day and historical traffic patterns. Save this script to a file named `external_scaler_predictive_client.py`:

```{literalinclude} ../doc_code/external_scaler_predictive_client.py
:language: python
:start-after: __client_script_begin__
:end-before: __client_script_end__
```

The script uses the external scaling API endpoint to scale deployments:
- **API endpoint**: `POST http://localhost:8265/api/v1/applications/{application_name}/deployments/{deployment_name}/scale`
- **Request body**: `{"target_num_replicas": <number>}` (must conform to the [`ScaleDeploymentRequest`](../api/doc/ray.serve.schema.ScaleDeploymentRequest.rst) schema)

The scaling client continuously adjusts the number of replicas based on the time of day:
- Business hours (9 AM - 5 PM): 10 replicas
- Off-peak hours: 3 replicas

##### Run the example

Follow these steps to run the complete example:

1. Start the Ray Serve application with the configuration:

```bash
serve run external_scaler_config.yaml
```

2. Run the predictive scaling client in a separate terminal:

```bash
python external_scaler_predictive_client.py
```

The client adjusts replica counts automatically based on the time of day. You can monitor the scaling behavior in the Ray dashboard or by checking the application logs.

#### Important considerations

Understanding how the external scaler interacts with your deployments helps you build reliable scaling logic:

- **Idempotent API calls**: The scaling API is idempotent. You can safely call it multiple times with the same `target_num_replicas` value without side effects. This makes it safe to run your scaling logic on a schedule or in response to repeated metric updates.

- **Interaction with serve deploy**: When you upgrade your service with `serve deploy`, the number of replicas you set through the external scaler API stays intact. This behavior matches what you'd expect from Ray Serve's built-in autoscaler—deployment updates don't reset replica counts.

- **Query current replica count**: You can get the current number of replicas for any deployment by querying the GET `/applications` API:

  ```bash
  curl -X GET http://localhost:8265/api/serve/applications/ \
  ```

  The response follows the [`ServeInstanceDetails`](../api/doc/ray.serve.schema.ServeInstanceDetails.rst) schema, which includes an `applications` field containing a dictionary with application names as keys. Each application includes detailed information about all its deployments, including current replica counts. Use this information to make informed scaling decisions. For example, you might scale up gradually by adding a percentage of existing replicas rather than jumping to a fixed number.

- **Initial replica count**: When you deploy an application for the first time, Ray Serve creates the number of replicas specified in the `num_replicas` field of your deployment configuration. The external scaler can then adjust this count dynamically based on your scaling logic.
