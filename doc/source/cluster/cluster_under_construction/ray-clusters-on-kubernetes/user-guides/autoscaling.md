(kuberay-autoscaler-discussion)=
# Autoscaling
This page discusses autoscaling in the context of Ray on Kubernetes.
For details on autoscaler configuration see the {ref}`configuration guide<kuberay-autoscaling-config>`.

:::{note}
Autoscaling is supported only with Ray versions at least
as new  as 1.11.0.
:::

(autoscaler-pro-con)=
## Should I enable autoscaling?
Ray Autoscaler support is optional.
Here are some considerations to keep in mind when choosing whether to use autoscaling.

### Autoscaling: Pros
**Cope with unknown resource requirements.** If you don't know how much compute your Ray
workload will require, autoscaling can adjust your Ray cluster to the right size.

**Save on costs.** Idle compute is automatically scaled down, potentially leading to cost savings,
especially when you are using expensive resources like GPUs.

### Autoscaling: Cons
**Less predictable when resource requirements are known.** If you already know exactly
how much compute your workload requires, it could make sense to provision a static Ray cluster
of the appropriate fixed size.

**Longer end-to-end runtime.** Autoscaling entails provisioning compute for Ray workers
while the Ray application is running. On the other hand, if you pre-provision a fixed
number of Ray workers, all of the Ray workers can be started in parallel, potentially reducing your application's
runtime.

## Ray Autoscaler vs. other autoscalers
We describe the relationship between the Ray autoscaler and other autoscalers in the Kubernetes
ecosystem.

### Ray Autoscaler vs. Horizontal Pod Autoscaler
The Ray autoscaler adjusts the number of Ray nodes in a Ray cluster.
On Kubernetes, each Ray node is run as a Kubernetes pod. Thus in the context of Kubernetes,
the Ray autoscaler scales Ray **pod quantities**. In this sense, the Ray autoscaler
plays a role similar to that of the Kubernetes
[Horizontal Pod Autoscaler](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/) (HPA).
However, the following features distinguish the Ray Autoscaler from the HPA.
#### Load metrics are based on application semantics
The Horizontal Pod Autoscaler determines scale based on physical usage metrics like CPU
and memory. By contrast, the Ray autoscaler uses the logical resources expressed in
task and actor annotations. For instance, if each Ray container spec in your RayCluster CR indicates
a limit of 10 CPUs, and you submit twenty tasks annotated with `@ray.remote(num_cpus=5)`,
10 Ray pods will be created to satisfy the 100-CPU resource demand.
In this respect, the Ray autoscaler is similar to the
[Kuberentes Cluster Autoscaler](https://github.com/kubernetes/autoscaler/tree/master/cluster-autoscaler),
which makes scaling decisions based on the logical resources expressed in container
resource requests.
#### Fine-grained control of scale-down
To accommodate the statefulness of Ray applications, the Ray autoscaler has more
fine-grained control over scale-down than the Horizontal Pod Autoscaler. In addition to
determining desired scale, the Ray Autoscaler is able to select precisely which pods
to scale down. The KubeRay operator then deletes that pod.
By contrast, the Horizontal Pod Autoscaler can only decrease a replica count, without much
control over which pods are deleted. For a Ray application, downscaling a random
pod could be dangerous.
#### Architecture: One Ray Autoscaler per Ray Cluster.
Horizontal Pod Autoscaling is centrally controlled by a manager in the Kubernetes control plane;
the manager controls the scale of many Kubernetes objects.
By contrast, each Ray cluster is managed by its own Ray autoscaler process,
running as a sidecar container in the Ray head pod. This design choice is motivated
by the following considerations:
- **Scalability.** Autoscaling each Ray cluster requires processing a significant volume of resource
  data from that Ray cluster.
- **Simplified versioning and compatibility.** The autoscaler and Ray are both developed
  as part of the Ray repository. The interface between the autoscaler and the Ray core is complex.
  To support multiple Ray clusters running at different Ray versions, it is thus best to match
  Ray and Autoscaler code versions. Running one autoscaler per Ray cluster and matching the code versions
  ensures compatibility.

### Ray Autoscaler with Kubernetes Cluster Autoscaler
The Ray Autoscaler and the
[Kubernetes Cluster Autoscaler](https://github.com/kubernetes/autoscaler/tree/master/cluster-autoscaler)
complement each other.
After the Ray autoscaler decides to create a Ray pod, the Kubernetes Cluster Autoscaler
can provision a Kubernetes node so that the pod can be placed.
Similarly, after the Ray autoscaler decides to delete an idle pod, the Kubernetes
Cluster Autoscaler can clean up the idle Kubernetes node that remains.
It is recommended to configure your RayCluster so that only one Ray pod fits per Kubernetes node.
If you follow this pattern, Ray Autoscaler pod scaling events will correspond roughly one-to-one with cluster autoscaler
node scaling events. (We say "roughly" because it is possible for a Ray pod be deleted and replaced
with a new Ray pod before the underlying Kubernetes node is scaled down.)


### Vertical Pod Autoscaler
There is no relationship between the Ray Autoscaler and the Kubernetes
[Vertical Pod Autoscaler](https://github.com/kubernetes/autoscaler/tree/master/vertical-pod-autoscaler) (VPA),
which is meant to size individual pods to the appropriate size based on current and past usage.
If you find that the load on your individual Ray pods is too high, there are a number
of manual techniques to decrease the load
One method is to schedule fewer tasks/actors per node by increasing the resource
requirements specified in the `ray.remote` annotation.
For example, changing `@ray.remote(num_cpus=2)` to `@ray.remote(num_cpus=4)`
will halve the quantity of that task or actor that can fit in a given Ray pod.

## Autoscaling architecture
The following diagram illustrates the integration of the Ray Autoscaler
with the KubeRay operator.
```{eval-rst}
.. image:: /cluster/cluster_under_construction/ray-clusters-on-kubernetes/images/AutoscalerOperator.svg
    :align: center
..
    Find the source document here (https://docs.google.com/drawings/d/1LdOg9JQuN5AOII-vDpSaFBsTeg0JGWcsbyNNLP1yovg/edit)
```

Worker pod upscaling occurs through the following sequence of events:
1. The user submits a Ray workload.
2. Workload resource requirements are aggregated by the Ray head container
   and communicated to the Ray autoscaler sidecar.
3. The autoscaler determines that a Ray worker pod must be added to satisfy the workload's resource requirement.
4. The autoscaler requests an addtional worker pod by incrementing the RayCluster CR's `replicas` field.
5. The KubeRay operator creates a Ray worker pod to match the new `replicas` specification.
6. The Ray scheduler places the user's workload on the new worker pod.

See also the operator architecture diagram in the [KubeRay documentation](https://ray-project.github.io/kuberay/components/operator/).
