(serve-autoscaling)=

# Serve Autoscaling

This section should help you:

- Learn how to use the Serve Autoscaling API.
- Learn how to optimize the Serve Autoscaling parameters for your workload.

To learn about the architecture underlying Ray Serve Autoscaling, see {ref}`serve-autoscaling-architecture`.

## Autoscaling parameters

There are several parameters the autoscaling algorithm takes into consideration when deciding the target number of replicas for your deployment:

**min_replicas[default_value=1]**: The minimal number of replicas for the deployment. ``min_replicas`` will also be the initial number of replicas when the deployment is deployed.
:::{note}
Ray Serve Autoscaling allows the `min_replicas` to be 0 when starting your deployment; the scale up will be started when you start sending traffic. There will be a cold start time as the Ray ServeHandle waits (blocks) for available replicas to assign the request.
:::
**max_replicas[default_value=1]**: Max replicas is the maximum number of replicas for the deployment. Ray Serve Autoscaling will rely on the Ray Autoscaler to scale up more nodes when the currently available cluster resources (CPUs, GPUs, etc) are not enough to bring up more replicas.

**target_num_ongoing_requests_per_replica[default_value=1]**: The config is to maintain how many ongoing requests are expected to run concurrently per replica at most. If the number is lower, the scale up will be done more aggressively.
:::{note}
- It is always recommended to load test your workloads. For example, if the use case is latency sensitive, you can lower the `target_num_ongoing_requests_per_replica` number to maintain high performance.
- Internally, the autoscaler will decide to scale up or down by comparing `target_num_ongoing_requests_per_replica` to the number of `RUNNING` and `PENDING` tasks on each replica.
- `target_num_ongoing_requests_per_replica` is only a target value used for autoscaling (not a hard limit), the real ongoing requests number can be higher than the config.
:::

**downscale_delay_s[default_value=600.0]**: The config is to control how long the cluster needs to wait before scaling down replicas.

**upscale_delay_s[default_value=30.0]**: The config is to control how long the cluster need to wait before scaling up replicas.
:::{note}
`downscale_delay_s` and `upscale_delay_s` are to control the frequency of doing autoscaling work. E.g. if your use case takes a long time to do initialization work, you can increase `downscale_delay_s` to make the down scaling happen slowly.
:::
**smoothing_factor[default_value=1]**: The multiplicative factor to speedup/slowdown each autoscaling step. E.g. When the use case has high large traffic volume in short period of time, you can increase `smoothing_factor` to scale up the resource quickly.

**metrics_interval_s[default_value=10]**: This is control how often each replica sends metrics to the autoscaler. (Normally you don't need to change this config.)
