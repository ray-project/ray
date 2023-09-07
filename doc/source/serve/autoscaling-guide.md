(serve-autoscaling)=

# Autoscaling

## Scaling horizontally with `num_replicas`

Each deployment consists of a number of [replicas](serve-architecture-high-level-view). To scale out your deployment without using autoscaling, increase the number of replicas using `num_replicas`.
The number of replicas is specified by the `num_replicas` field in the deployment options.
By default, `num_replicas` is 1.

```{literalinclude} doc_code/managing_deployments.py
:start-after: __scaling_out_start__
:end-before: __scaling_out_end__
:language: python
```

## Autoscaling Quick Start

Instead of setting a fixed number of replicas for a deployment, you can configure a deployment to autoscale based on incoming traffic. The Serve autoscaler reacts to traffic spikes by monitoring queue sizes and making scaling decisions to add or remove replicas. Configure it by setting the [autoscaling_config](../serve/api/doc/ray.serve.config.AutoscalingConfig.rst) field in deployment options. The following is a quick guide on how to set your autoscaling config.

* Set **`target_num_ongoing_requests_per_replica`** to a reasonable number (for example, 5) and adjust it based on your request processing length (the longer the requests, the smaller this number should be) as well as your latency objective (the shorter you want your latency to be, the smaller this number should be).
* Set **`max_concurrent_queries`** to a value ~20-50% greater than `target_num_ongoing_requests_per_replica`.
* Set **`min_replicas`** to 0 if there are long periods of no traffic and some extra tail latency during upscale is acceptable. Otherwise, set this to what you think you need for low-traffic.
* Set **`max_replicas`** to ~20% higher than what you think you need for peak traffic.

This would be a great starting point. If you decide to further tune your autoscaling config for your application, you can read on to following sections.

## Autoscaling Concepts Overview

Before diving into an example, we will go over some foundational Serve autoscaling concepts.

:::{note}
The Ray Serve Autoscaler is an application-level autoscaler that sits on top of the [Ray Autoscaler](cluster-index).
Concretely, this means that the Ray Serve autoscaler asks Ray to start a number of replica actors based on the request demand.
If the Ray Autoscaler determines there aren't enough available resources (e.g. CPUs, GPUs, etc.) to place these actors, it responds by requesting more Ray nodes.
The underlying cloud provider will then respond by adding more nodes.
Similarly, when Ray Serve scales down and terminates replica actors, it will attempt to make as many nodes idle as possible so the Ray autoscaler can remove them. To learn more about the architecture underlying Ray Serve Autoscaling, see {ref}`serve-autoscaling-architecture`.
:::

### Define the Steady State of your System

To define what the steady state of your deployments should be like, you need to set values for `target_num_ongoing_requests_per_replica` and `max_concurrent_queries`.

Serve will scale the number of replicas for a deployment up or down based on the average number of ongoing requests per replica. Specifically, Serve will compare the *actual* number of ongoing requests per replica with the target value you set in the autoscaling config and make upscale or downscale decisions from that. The target value is set by `target_num_ongoing_requests_per_replica`, and Serve will try to make sure that each replica has roughly that number
of requests being processed and waiting in the queue. We recommend you benchmark your application code and set this number based on an end to end latency objective.

:::{tip}
- It is always recommended to load test your workloads. For example, if the use case is latency sensitive, you can lower the `target_num_ongoing_requests_per_replica` number to maintain high performance.
- Internally, the autoscaler will decide to scale up or down by comparing `target_num_ongoing_requests_per_replica` to the number of requests running and pending on each replica.
- `target_num_ongoing_requests_per_replica` is only a target value used for autoscaling (not a hard limit), the real ongoing requests number can be higher than the config.
:::

:::{note}
As an example, suppose you have two replicas of a synchronous deployment that has 100ms latency, serving a traffic load of 30 QPS. Then requests will be assigned to replicas faster than the replicas can finish processing them; more and more requests will be queued up at the replica (these are considered "ongoing requests") as time goes on, and so the average number of ongoing requests at each replica will steadily increase. If you set `target_num_ongoing_requests_per_replica = 1`, Serve will detect a higher-than-desired number of ongoing requests per replica, and add more replicas. At 3 replicas, your system would be able to process 30 QPS with 1 ongoing request per replica on average.
:::

There is also a maximum limit for the queue that is respected by proxies when assigning requests to replicas, which is defined by `max_concurrent_queries`. We recommend setting `max_concurrent_queries` to ~20 to 50% higher than `target_num_ongoing_requests_per_replica`. Note that `target_num_ongoing_requests_per_replica` should always be strictly less than `max_concurrent_queries`, otherwise the deployment will never scale up. You should take into account the following when setting `max_concurrent_queries`:

- If it's set too low, then it will limit upscaling. For instance, if your target value is 50 and `max_concurrent_queries` is 51, then even if the traffic increases significantly, the requests will queue up at the proxy instead of at the replicas. As a result, the autoscaler will only increase the number of replicas at most 2% at a time, which is very slow.
- On the other hand, it it's set too high, this can lead to imbalanced routing. If the autoscaler is scaling a deployment up due to a traffic spike, most or all of the requests might be assigned to the existing replicas before the new replicas are started, which can lead to very high tail latencies during upscale.

### Define Upper and Lower Autoscaling Limits

To use autoscaling, you need to define the minimum and maximum number of resources allowed for your system.

* **`min_replicas`**: This is the minimum number of replicas for the deployment. If you want to ensure your system can deal with a certain level of traffic at all times, you can set `min_replicas` to a positive number. On the other hand, if you anticipate periods of no traffic and want to scale to zero to save costs, you can set `min_replicas = 0`. Note that setting `min_replicas = 0` will cause higher tail latencies.
* **`max_replicas`**: This is the maximum number of replicas for the deployment. This should be greater than `min_replicas`.
* **`initial_replicas`**: This is the number of replicas that will be started initially for the deployment.


### [Optional] Define how the System reacts to changing traffic

Given a steady stream of traffic and appropriately configured `min_replicas` and `max_replicas`, the steady state of your system is essentially fixed for a chosen configuration value for `target_num_ongoing_requests_per_replica`. Before reaching steady state, however, your system will be reacting to traffic shifts. How you want your system to react to changes in traffic determines how you want to set the remaining autoscaling configurations. We will go over two more commonly used parameters here.

* **`upscale_delay_s [default=30s]`**: This defines how long Serve waits before scaling up the number of replicas in your deployment. More specifically, if the replicas are *consistently* serving more requests than desired for an `upscale_delay_s` number of seconds, then Serve will scale up the number of replicas based on aggregated ongoing requests metrics.
* **`downscale_delay_s [default=600s]`**: This defines how long Serve waits before scaling down the number of replicas in your deployment. More specifically, if the replicas are *consistently* serving less requests than desired for a `downscale_delay_s` number of seconds, then Serve will scale down the number of replicas based on aggregated ongoing requests metrics.


## Basic Example

Let's go through an example synchronous workload that runs Resnet50. This is the application code:
```{literalinclude} doc_code/resnet50_example.py
:language: python
:start-after: __serve_example_begin__
:end-before: __serve_example_end__
```

This is the autoscaling configuration. We set `target_num_ongoing_requests_per_replica = 1` because the resnet model runs synchronously, and we want to keep the latencies low. Note that the deployment starts with 0 replicas.
```yaml
applications:
  - name: default
    import_path: resnet:app
    deployments:
    - name: Model
      max_concurrent_queries: 10
      autoscaling_config:
        target_num_ongoing_requests_per_replica: 1
        min_replicas: 0
        initial_replicas: 0
        max_replicas: 200
        upscale_delay_s: 30
        downscale_delay_s: 600
```

We use [Locust](https://locust.io/) to run a load test against this application. The Locust load test will run a certain number of "users" that ping the Resnet50 service, where each user has a [constant wait time](https://docs.locust.io/en/stable/writing-a-locustfile.html#wait-time-attribute) of 0. This means each user will (repeatedly) send a request, wait for a response, then immediately send the next request. The number of users running over time is shown in the following graph:

![users](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/resnet50_users.png)

The results of the load test are as follows:

|  |  |  |
| -------- | --- | ------- |
| Replicas | <img src="https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/resnet50_replicas.png" alt="replicas" width="600"/> |
| QPS | <img src="https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/resnet50_rps.svg" alt="qps"/> |
| P50 Latency | <img src="https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/resnet50_latency.svg" alt="latency"/> |

Notice the following:
- Each Locust user constantly sends a single request and waits for a response. As a result, the number of autoscaled replicas closely matches the number of Locust users over time as Serve attempts to satisfy the `target_num_ongoing_requests_per_replica=1` setting.
- The throughput of the system increases with the number of users and replicas.
- The latency briefly spikes when traffic increases, but otherwise stays relatively steady. The biggest latency spike happens at the beginning of the test, because the deployment starts with 0 replicas.


## Model Composition Example

Determining the autoscaling configuration for a multi-model application requires understanding each deployment's scaling requirements. Every deployment has a different latency and differing levels of concurrency. As a result, it takes experimentation to find the right autoscaling config for a model-composition application.

We will go through an example of a simple application with three deployments composed together to build some intuition about multi-model autoscaling. Suppose we have deployments:
* `HeavyLoad`: A mock 200ms workload with high CPU usage.
* `LightLoad`: A mock 100ms workload with high CPU usage.
* `Driver`: A driver deployment that fans out to the `HeavyLoad` and `LightLoad` deployments and aggregates the two outputs.


### Attempt 1: One `Driver` replica

Let's start off with the following deployment configurations. Since the driver deployment has low CPU usage and is only asynchronously making calls to the downstream deployments, it's reasonable to allocate one fixed `Driver` replica. Some parameters in the autoscaling configuration for `HeavyLoad` and `LightLoad` were not covered above, but they are used in this test in order to deliver best results. Please refer to [Autoscaling Config Parameters](serve-autoscaling-config-parameters) below for an overview of the parameters.

::::{tab-set}

:::{tab-item} Driver

```yaml
- name: Driver
  num_replicas: 1
  max_concurrent_queries: 200
```

:::

:::{tab-item} HeavyLoad

```yaml
- name: HeavyLoad
  max_concurrent_queries: 3
  autoscaling_config:
    target_num_ongoing_requests_per_replica: 1
    min_replicas: 0
    initial_replicas: 0
    max_replicas: 200
    upscale_delay_s: 3
    downscale_delay_s: 60
    upscale_smoothing_factor: 0.3
    downscale_smoothing_factor: 0.3
    metrics_interval_s: 2
    look_pack_period_s: 10
```

:::

:::{tab-item} LightLoad

```yaml
- name: LightLoad
  max_concurrent_queries: 3
  autoscaling_config:
    target_num_ongoing_requests_per_replica: 1
    min_replicas: 0
    initial_replicas: 0
    max_replicas: 200
    upscale_delay_s: 3
    downscale_delay_s: 60
    upscale_smoothing_factor: 0.3
    downscale_smoothing_factor: 0.3
    metrics_interval_s: 2
    look_pack_period_s: 10
```

:::

::::


Running the same Locust load test as we did for the Resnet workload, we get the following results:

| | |
| ----------------------- | ---------------------- |
| HeavyLoad and LightLoad Number Replicas | <img src="https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/model_composition_replicas.png" alt="comp" width="500" /> |


As one might expect, the number of autoscaled `LightLoad` replicas is roughly half that of autoscaled `HeavyLoad` replicas. Although the same number of requests per second are getting sent to both deployments, `LightLoad` replicas can process twice as many requests per second as `HeavyLoad` replicas can, and so the deployment should need half as many replicas to deal with the same traffic load.

However, the service latency rises to 400+ ms when the number of Locust users increased to 100.

| P50 Latency | QPS |
| ------- | --- |
| ![comp_latency](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/model_comp_latency.svg) | ![comp_rps](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/model_comp_rps.svg) |

Note that the number of `HeavyLoad` replicas should roughly match the number of Locust users to adequately serve the Locust traffic. However, when the number of Locust users increased to 100, the `HeavyLoad` deployment struggled to reach 100 replicas, and instead only reached 65 replicas. The per-deployment latencies reveal the root cause. While `HeavyLoad` and `LightLoad` latencies stayed steady at 200ms and 100ms, `Driver` latencies rose from 220 to 400+ ms. This suggests that high Locust workload may be overwhelming the `Driver` replica, potentially by impacting the asynchronous event loop's performance.


### Attempt 2: Autoscale `Driver`

Let's try setting an autoscaling configuration for `Driver` as well, with `target_num_ongoing_requests_per_replica = 20`. Now the deployment configurations are as follows.

::::{tab-set}

:::{tab-item} Driver

```yaml
- name: Driver
  max_concurrent_queries: 200
  autoscaling_config:
    target_num_ongoing_requests_per_replica: 20
    min_replicas: 1
    initial_replicas: 1
    max_replicas: 10
    upscale_delay_s: 3
    downscale_delay_s: 60
    upscale_smoothing_factor: 0.3
    downscale_smoothing_factor: 0.3
    metrics_interval_s: 2
    look_pack_period_s: 10
```

:::

:::{tab-item} HeavyLoad

```yaml
- name: HeavyLoad
  max_concurrent_queries: 3
  autoscaling_config:
    target_num_ongoing_requests_per_replica: 1
    min_replicas: 0
    initial_replicas: 0
    max_replicas: 200
    upscale_delay_s: 3
    downscale_delay_s: 60
    upscale_smoothing_factor: 0.3
    downscale_smoothing_factor: 0.3
    metrics_interval_s: 2
    look_pack_period_s: 10
```

:::

:::{tab-item} LightLoad

```yaml
- name: LightLoad
  max_concurrent_queries: 3
  autoscaling_config:
    target_num_ongoing_requests_per_replica: 1
    min_replicas: 0
    initial_replicas: 0
    max_replicas: 200
    upscale_delay_s: 3
    downscale_delay_s: 60
    upscale_smoothing_factor: 0.3
    downscale_smoothing_factor: 0.3
    metrics_interval_s: 2
    look_pack_period_s: 10
```

:::
::::

Running the same Locust load test again, we get the following results:

| | |
| ------------------------------------ | ------------------- |
| HeavyLoad and LightLoad Number Replicas | <img src="https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/model_composition_improved_replicas.png" alt="heavy" width="500"/> |
| Driver Number Replicas | <img src="https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/model_composition_improved_driver_replicas.png" alt="driver" width="500"/>

With up to 6 `Driver` deployments to receive and distribute the incoming requests, the `HeavyLoad` deployment successfully scales up to 90+ replicas, and `LightLoad` up to 47 replicas. This helps the application latency stay consistent as the traffic load increases.

| Improved P50 Latency | Improved RPS |
| ---------------- | ------------ |
| ![comp_latency](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/model_composition_improved_latency.svg) | ![comp_latency](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/model_comp_improved_rps.svg) |


(serve-autoscaling-config-parameters)=
## Autoscaling Config Parameters

There are several user-specified parameters the autoscaling algorithm takes into consideration when making autoscaling decisions:

**min_replicas[default_value=1]**: The minimum number of replicas for the deployment. Ray Serve allows this to be set to 0, which means there can be 0 replicas in periods of inactivity. When you start sending traffic, the deployment will scale up, and there will be a cold start time as Serve waits for replicas to be started to serve the request.

**max_replicas[default_value=1]**: The maximum number of replicas for the deployment. Ray Serve Autoscaling will rely on the Ray Autoscaler to scale up more nodes when the currently available cluster resources (CPUs, GPUs, etc.) are not enough to support more replicas.

**target_num_ongoing_requests_per_replica[default_value=1]**: How many ongoing requests are expected to run concurrently per replica. The autoscaler scales up if the value is lower than the current number of ongoing requests per replica. Similarly, the autoscaler scales down if it's higher than the current number of ongoing requests. Scaling happens quicker if there's a high disparity between this value and the current number of ongoing requests.

**downscale_delay_s[default_value=600.0]**: How long the cluster needs to wait before scaling down replicas. This controls the frequency of downscale decisions. For example, if your application initializes slowly, you can increase `downscale_delay_s` to make the downscaling happen more infrequently and avoid reinitialization when the application needs to upscale again in the future.

**upscale_delay_s[default_value=30.0]**: How long the cluster needs to wait before scaling up replicas. This controls the frequency of upscale decisions.

**upscale_smoothing_factor[default_value=1.0]**: The multiplicative factor to speed up or slow down each upscaling decision. For example, when the application has high traffic volume in a short period of time, you can increase `upscale_smoothing_factor` to scale up the resource quickly. You can think of this as a "gain" factor to amplify the response of the autoscaling algorithm.

**downscale_smoothing_factor[default_value=1.0]**: The multiplicative factor to speed up or slow down each downscaling decision. For example, if you want your application to be less sensitive to drops in traffic and scale down more conservatively, you can decrease `downscale_smoothing_factor` to slow down the pace of downscaling.

**metrics_interval_s[default_value=10]**: This controls how often each replica sends reports on current ongoing requests to the autoscaler. Note that the autoscaler cannot make new decisions if it does not receive updated metrics, so you most likely want to set `metrics_interval_s` to a value that is at most the upscale and downscale delay values. For instance, if you set `upscale_delay_s = 3`, but keep `metrics_interval_s = 10`, the autoscaler will only upscale roughly every 10 seconds.

**look_back_period_s[default_value=30]**: This controls how often each replica sends reports on current ongoing requests to the autoscaler. Note that the autoscaler cannot make new decisions if it does not receive updated metrics, so you most likely want to set `metrics_interval_s` to a value that is at most the upscale and downscale delay values. For instance, if you set `upscale_delay_s = 3`, but keep `metrics_interval_s = 10`, the autoscaler will only upscale roughly every 10 seconds.


## Troubleshooting Guide


### Number of Autoscaled Replicas not Stable

If you are seeing that the number of replicas in your deployment keeps oscillating even though the traffic is relatively stable, here are some things you can try:

* Set a smaller `upscale_smoothing_factor` and `downscale_smoothing_factor`.
* Set a `look_back_period_s` value that matches the rest of the autoscaling config. For longer upscale and downscale delay values, a longer look back period can likely help stabilize the replica graph, but for shorter upscale and downscale delay values, a shorter look back period may be more appropriate. For instance, the following replica graphs show how a deployment with `upscale_delay_s = 3` works with a longer vs shorter look back period.

| `look_back_period_s = 30` | `look_back_period_s = 3` |
| ------------------------------------------------ | ----------------------------------------------- |
| ![look-back-before](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/look_back_period_before.png) | ![look-back-after](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/look_back_period_after.png) |


### High spikes in latency during bursts of traffic

If you expect your application to receive bursty traffic, and at the same time want the deployments to scale down in periods of inactivity, you are likely concerned about how quickly the deployment can scale up and respond to bursts of traffic. While an increase in latency initially during a burst in traffic may be unavoidable, here are some things you can try to improve latency during bursts of traffic.

* Set a lower `upscale_delay_s`. The autoscaler will always wait `upscale_delay_s` seconds before making a decision to upscale, so lowering this delay allows the autoscaler to react more quickly to changes, especially bursts, of traffic.
* Set a larger `upscale_smoothing_factor`. If `upscale_smoothing_factor > 1`, then the autoscaler will scale up more aggressively than normal. This can allow your deployment to be more sensitive to bursts of traffic.
* Lower the `metric_interval_s`. we recommend to always set `metric_interval_s` to be less than or equal to `upscale_delay_s`, otherwise upscaling will be delayed because the autoscaler doesn't receive fresh information often enough.
* Set a lower `max_concurrent_queries`. If `max_concurrent_queries` is too high relative to `target_num_ongoing_requests_per_replica`, then when traffic increases, most or all of the requests might be assigned to the existing replicas before the new replicas are started. This can lead to very high latencies during upscale.


### Want deployment to scale down more conservatively

You may observe that deployments are scaling down too quickly for your liking. Instead, you may want the downscaling to be much more conservative to maximize the availability of your service.

* Set a longer `downscale_delay_s`. The autoscaler will always wait `downscale_delay_s` seconds before making a decision to downscale, so by increasing this number, your system will have a longer "grace period" after traffic drops before the autoscaler starts to remove replicas.
* Set a smaller `downscale_smoothing_factor`. If `downscale_smoothing_factor < 1`, then the autoscaler will remove *less replicas* than what it thinks it should remove to achieve the target number of ongoing requests. In other words, the autoscaler makes more conservative downscaling decisions.

| `downscale_smoothing_factor = 1` | `downscale_smoothing_factor = 0.5` |
| ------------------------------------------------ | ----------------------------------------------- |
| ![downscale-smooth-before](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/downscale_smoothing_factor_before.png) | ![downscale-smooth-after](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling-guide/downscale_smoothing_factor_after.png) |