(autoscaling)=
# Autoscaling

The Ray autoscaler is a Ray cluster process that automatically scales a cluster up and down based on resource demand.
The autoscaler does this by adjusting the number of nodes in the cluster based on the resources required by tasks, actors or placement groups.

:::{note}
Autoscaling is supported only with Ray versions at least
as new  as 1.11.0.
:::

Note that the autoscaler only considers logical resource requests for scaling (i.e., those specified in ``@ray.remote`` and displayed in `ray status`), not physical machine utilization. If a user tries to launch an actor, task, or placement group but there are insufficient resources, the request will be queued. The autoscaler adds nodes to satisfy resource demands in this queue.
The autoscaler also removes nodes after they become idle for some time.
A node is considered idle if it has no active tasks or actors and no objects.

(autoscaler-pro-con)=
## Should I enable autoscaling?
Ray Autoscaler support is optional.
Here are some considerations to keep in mind when choosing whether to use autoscaling.

### Autoscaling: Pros
**Cope with unknown resource requirements.** If you don't know how much compute your Ray
workload will require, autoscaling will adjust your Ray cluster to the right size.

**Save on costs.** Idle compute is automatically scaled down, potentially leading to cost savings,
especially when you are using expensive resources like GPUs.

### Autoscaling: Cons
**Less predictable when resource requirements are known.** If you already know exactly
how much compute your workload requires, it may be simpler to provision a fixed Ray cluster.

**Longer end-to-end runtime.** Autoscaling entails provisioning compute for Ray workers
while the Ray application is running. On the other hand, if you pre-provision a fixed
number of Ray workers, all of the Ray workers can be started in parallel, potentially reducing your application's
run time.

## Getting Started with Autoscaling

You can enable autoscaling by configuring a min and max number of nodes in your Ray cluster instead of a fixed number of nodes.
The autoscaler configuration also provides more advanced options for tuning the speed of upscaling and downscaling.

To learn how to enable autoscaling, check out the guides for configuring Ray autoscaling when using the [Kubernetes](kuberay-autoscaling) or [VM-based cluster launcher](vms-autoscaling).

Once autoscaling has been enabled in your Ray cluster, you can run your application as usual. In some cases, you may want finer-grained control over when and what type of node should be added to the cluster. For these cases, you can also manually request resources from the autoscaler. Check out the [autoscaler Python SDK](ref-autoscaler-sdk-request-resources-under-construction) for more details.

[//]: <> (TODO: we should include an example using request_resources and/or end-to-end examples of configuring a cluster and running an autoscaling application)

```{toctree}
:maxdepth: '1'

../../kubernetes/user-guides/configuring-autoscaling
../../vms/user-guides/configuring-autoscaling
reference
```
