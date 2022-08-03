(kuberay-autoscaler-discussion)=
# Autoscaling
This page discusses autoscaling in the context of Ray on Kubernetes.

(autoscaler-pro-con)=
## Should I enable autoscaling?
Ray autoscaler support is optional.
Here are some considerations to keep in mind when choosing whether to use autoscaling.

### Autoscaling: Pros
**Cope with unknown resource requirements** If you don't know how much compute your Ray
workload will require, autoscaling can adjust your Ray cluster to the right size.
**Save on costs** Idle compute is automatically scaled down, potentially leading to cost savings,
especially when you are using expensive resources like GPUs.

### Autoscaling: Cons
**Less predictable when resource requirements are known.** If you already know exactly
how much compute your workload requires, it makes sense to provision a statically-sized Ray cluster.

**Longer end-to-end runtime.** Autoscaling entails provisioning compute for Ray workers
while the Ray application is running. On the other hand, if you pre-provision a fixed
number of Ray nodes,
all of the Ray nodes can be started in parallel, potentially reducing your application's
runtime.

**Resource usage.** The autoscaler itself uses CPU and memory resources
which must be accounted for when considering the Ray pod's scheduling.
(The defaults are .5 CPU requests and limits, 512 megabyte memory request and limits.)

## Ray Autoscaler vs. other autoscalers
We describe the relationship of the Ray autoscaler to other autoscalers in the Kubernetes
ecosystem.

### Ray Autoscaler vs. Horizontal Pod Autoscaler
The Ray Autoscaler adjusts the number of Ray nodes in a Ray cluster.
On Kubernetes, we run one Ray node per Kubernetes pod. Thus in the context of Kubernetes,
the Ray autoscaler scales Ray **pod quantities**. In this sense, the relationship between
the Ray autoscaler and KubeRay operator is similar to the Horizontal Pod Autoscaler and Deployment controller.
However, the Ray Autoscaler has two key aspects which distinguish it from the HPA.
1. *Application-specific load metrics*
The Horizontal Pod Autoscaler determines scale based on actual usage metrics like CPU
and memory. By contrast, the Ray autoscaler uses Ray's logical resources.
...
In terms of the metrics it takes into account, the Ray autoscaler is thus similar
to the Kubernetes cluster autoscaler based on the logical resources specified
in container resource requests.
2. *Fine-grained control of scale-down*
To accommodate the statefulness of Ray applications, the Ray autoscaler has more
fine-grained control over scale-down than the Horizontal Pod Autoscaler -- in addition to
determining desired scale, the Ray autoscaler is able to select.
3. *Architecture*
Horizontal Pod Autoscaling is centrally managed by a manager in the Kubernetes control plane;
the manager controls the scale of many Kubernetes Objects.
By contrast, each Ray cluster is managed by its own Ray Autoscaler
attached as a sidecar container in the Ray head pod.

### Ray Autoscaler with Kubernetes cluster autoscaler
The Ray autoscaler and Kubernetes cluster autoscaler complement each other.
After the Ray autoscaler decides to create a Ray pod, the cluster autoscaler
can scale up a Kubernetes node so that the pod can be placed.
Similarly, after the Ray autoscaler decides to delete an idle pod, the Kubernetes
cluster autoscaler can clean up the idle Kubernetes node that remains.
If you configure your RayCluster so that one Ray pod fits on a Kubernetes node,
Ray autoscaler pod scaling events will correspond roughly one-to-one with cluster autoscaler
node scaling events.


### Vertical pod autoscaler
There is no integration or relationship between the Ray and the Vertical Pod Autoscaler,
which is meant to size individual pods to the appropriate size.
If you find that the load on your individual Ray pods is too high, there are a number
of manual techniques to decrease the load.
One method in particular is to schedule fewer tasks/actors per node by increasing the resources specified in the ray.remote annotation.
For example, changing `@ray.remote(num_cpus=2)` to `@ray.remote(num_cpus=4)`.
will will halve the number of that task or actor that can fit in a given Ray pod.
