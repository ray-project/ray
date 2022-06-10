(serve-autoscaling)=

# Serve Autoscaling

This section should help you:

- Understand how Ray Serve autoscaling works
- Understand config parameter usage


## Autoscaling architecture
Ray Serve autoscaling is to increase and decrease the number of replicas based on the load, and ray serve provide a couple of config parameters to meet the different workload use cases.

![pic](https://raw.githubusercontent.com/ray-project/images/master/docs/serve/autoscaling.svg)

- Deployment handle(Client) and worker replica periodically push the stats to the autoscaler.
- Autoscaler requires serve handle queue metrics and replicas requests metrics to make decision whether to scale (up or down) the replicas.
- Deployment Handle(Client) keeps polling the replica stats from controller to get the updated replicas information. Serve Handle(Client) will send requests directly to the replica based on the replicas information (Round Robin).

:::{note}
When the controller dies, the client is still able to send requests, but autoscaling will pause to work. When the controller recovers, the autoscaling will start working, all previous metrics collected will be lost.
:::

## Autoscaling parameters
There are several parameters the autoscaling algorithm takes into consideration when deciding the target replicas for your deployment
**min_replicas**: The minimal number of replicas for the deployment, the min_replicas will also be the initial number replicas when the deployment is deployed.
:::{note}
Ray Serve Autoscaling allows the `min_replicas` to be 0 to start your deployment, the scale up will be started when you start sending traffic. There will be cold start time during the period, ray serve handle will wait (block) for available replicas to assign the request.
:::
**max_replicas**: Max replicas is the maximum number of replicas for the deployment. Ray Serve Autoscaling will rely on the Ray Autoscaler to scale up more nodes when the resource is not enough to bring up more replicas. 
**target_num_ongoing_requests_per_replica**: The config is to maintain how many ongoing requests concurrently running per replica at most. If the number is lower, the scale up will be done more aggressively.
:::{note}
- It is always recommended to load testing your workloads. For example, if the use case is latency sensitive, you can lower the `target_num_ongoing_requests_per_replica` number to maintain the high performance.
- Internally, the autoscaler will compare RUNNING + PENNING tasks of each replicas and `target_num_ongoing_requests_per_replica` to decide scale up/down.
:::
**downscale_delay_s**: The config is to control how long the cluster need to wait before scaling down replicas.
`upscale_delay_s`: The config is to control how long the cluster need to wait before scaling up replicas.
:::{note}
`downscale_delay_s` and `upscale_delay_s` is to control the frequency of doing autoscaling works. E.g. your use case is having long time to do initialization works, you can increase the `downscale_delay_s` to make the down scaling works slowly.
:::
**smoothing_factor**: The multiplicative factor to speedup/slowdown the autoscaling step each. E.g. When the use case has high large traffic volume in short period of time, user can increase the smoothing_factor to scale up the resource quickly.

**metrics_interval_s**: This is control the frequency of how long each replica sending metrics to the autoscaler. (Normally you don't need to change this config) 

