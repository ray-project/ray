(kuberay-autoscaling)=

# KubeRay Autoscaling

This guide explains how to configure the Ray autoscaler on Kubernetes.
The Ray autoscaler is a Ray cluster process that automatically scales a cluster up and down based on resource demand.
The autoscaler does this by adjusting the number of nodes (Ray pods) in the cluster based on the resources required by tasks, actors or placement groups.

Note that the autoscaler only considers logical resource requests for scaling (i.e., those specified in ``@ray.remote`` and displayed in `ray status`), not physical machine utilization. If a user tries to launch an actor, task, or placement group but there are insufficient resources, the request will be queued. The autoscaler adds nodes to satisfy resource demands in this queue.
The autoscaler also removes nodes after they become idle for some time.
A node is considered idle if it has no active tasks, actors, or objects.

<!-- TODO(ekl): probably should change the default kuberay examples to not use autoscaling -->
```{admonition} When to use Autoscaling?
Autoscaling can reduce workload costs, but adds node launch overheads and can be tricky to configure.
We recommend starting with non-autoscaling clusters if you're new to Ray.
```

## Overview
The following diagram illustrates the integration of the Ray Autoscaler
with the KubeRay operator.

```{eval-rst}
.. image:: ../images/AutoscalerOperator.svg
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

## Quickstart

First, follow the [quickstart guide](kuberay-quickstart) to create an autoscaling cluster. The commands to create the KubeRay operator and deploy an autoscaling cluster are summarized here:

```bash
# Optionally use kind to run the examples locally.
# $ kind create cluster

# Create the KubeRay operator. Make sure your Kubernetes cluster and Kubectl are both at version at least 1.19.
$ kubectl create -k "github.com/ray-project/kuberay/ray-operator/config/default?ref=v0.4.0&timeout=90s"
# Create an autoscaling Ray cluster.
$ kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-cluster.autoscaler.yaml
```

Now, we can run a Ray program on the head pod that uses [``request_resources``](ref-autoscaler-sdk) to scale the cluster to a total of 3 CPUs. The head and worker pods in our [example cluster config](https://github.com/ray-project/kuberay/blob/master/ray-operator/config/samples/ray-cluster.autoscaler.yaml) each have a capacity of 1 CPU, and we specified a minimum of 1 worker pod. Thus, the request should trigger upscaling of one additional worker pod.

Note that in real-life scenarios, you will want to use larger Ray pods. In fact, it is advantageous to size each Ray pod to take up an entire Kubernetes node. See the [configuration guide](kuberay-config) for more details.

To run the Ray program, we will first get the name of the Ray head pod:

```bash
$ kubectl get pods --selector=ray.io/cluster=raycluster-autoscaler --selector=ray.io/node-type=head -o custom-columns=POD:metadata.name --no-headers
# raycluster-autoscaler-head-xxxxx
```

Then, we can run the Ray program using ``kubectl exec``:
```bash
$ kubectl exec raycluster-autoscaler-head-xxxxx -it -c ray-head -- python -c "import ray; ray.init(); ray.autoscaler.sdk.request_resources(num_cpus=3)"
```

The last command should have triggered Ray pod upscaling. To confirm the new worker pod is up, let's query the RayCluster's pods again:

```bash
$ kubectl get pod --selector=ray.io/cluster=raycluster-autoscaler
# NAME                                             READY   STATUS    RESTARTS   AGE
# raycluster-autoscaler-head-xxxxx                 2/2     Running   0          XXs
# raycluster-autoscaler-worker-small-group-yyyyy   1/1     Running   0          XXs
# raycluster-autoscaler-worker-small-group-zzzzz   1/1     Running   0          XXs 
```

To get a summary of your cluster's status, run `ray status` on your cluster's Ray head node.
```bash
# Substitute your head pod's name in place of \"raycluster-autoscaler-head-xxxxx
$ kubectl exec raycluster-autoscaler-head-xxxxx -it -c ray-head -- ray status
# ======== Autoscaler status: 2022-07-21 xxxxxxxxxx ========
# ....
```

Alternatively, to examine the full autoscaling logs, fetch the stdout of the Ray head pod's autoscaler sidecar:
```bash
# This command gets the last 20 lines of autoscaler logs.
# Substitute your head pod's name in place of \"raycluster-autoscaler-head-xxxxx
$ kubectl logs raycluster-autoscaler-head-xxxxx -c autoscaler | tail -n 20
# ======== Autoscaler status: 2022-07-21 xxxxxxxxxx ========
# ...
```

(kuberay-autoscaling-config)=
## KubeRay Config Parameters

There are two steps to enabling Ray autoscaling in the KubeRay `RayCluster` custom resource (CR) config:

1. Set `enableInTreeAutoscaling:true`. The KubeRay operator will then automatically configure an autoscaling sidecar container
for the Ray head pod. The autoscaler container collects resource metrics from the Ray cluster
and automatically adjusts the `replicas` field of each `workerGroupSpec` as needed to fulfill
the requirements of your Ray application.

2. Set the fields `minReplicas` and `maxReplicas` to constrain the number of `replicas` of an autoscaling
`workerGroup`. When deploying an autoscaling cluster, one typically sets `replicas` and `minReplicas`
to the same value.
The Ray autoscaler will then take over and modify the `replicas` field as pods are added to or removed from the cluster.

For an example, check out the [config file](https://github.com/ray-project/kuberay/blob/master/ray-operator/config/samples/ray-cluster.autoscaler.yaml) that we used in the above quickstart guide.

### Upscaling and downscaling speed

If needed, you can also control the rate at which nodes should be added to or removed from the cluster. For applications with many short-lived tasks, you may wish to adjust the upscaling and downscaling speed to be more conservative.

Use the `RayCluster` CR's `autoscalerOptions` field to do so. The `autoscalerOptions` field
carries the following subfields:

**`upscalingMode`**: This controls the rate of Ray pod upscaling. The valid values are:
    - `Conservative`: Upscaling is rate-limited; the number of pending worker pods is at most the number
      of worker pods connected to the Ray cluster.
    - `Default`: Upscaling is not rate-limited.
    - `Aggressive`: An alias for Default; upscaling is not rate-limited.

**`idleTimeoutSeconds`** (default 60s): This is the number of seconds to wait before scaling down an idle worker pod. Worker nodes are considered idle when they hold no active tasks, actors, or referenced objects (either in-memory or spilled to disk).

### Configuring the autoscaler sidecar container

The `autoscalerOptions` field also provides options for configuring the autoscaler container. Usually, it is not necessary to specify these options.

**`resources`**: The `resources` subfield of `autoscalerOptions` sets optional resource overrides
for the autoscaler sidecar container. These overrides
should be specified in the standard [container resource
spec format](https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/#resources).
The default values are indicated below:
```
resources:
  limits:
    cpu: "500m"
    memory: "512Mi"
  requests:
    cpu: "500m"
    memory: "512Mi"
```

The following `autoscalerOptions` suboptions are also available for testing and development of the autoscaler itself.

**`image`**: This field overrides the autoscaler container image.
If your `RayCluster`'s `spec.RayVersion` is at least `2.0.0`, the autoscaler will default to using
**the same image** as the Ray container. (Ray autoscaler code is bundled with the rest of Ray.)
For older Ray versions, the autoscaler will default to the image `rayproject/ray:2.0.0`.

**`imagePullPolicy`**: This field overrides the autoscaler container's
image pull policy. The default is `Always`.

**`env`** and **`envFrom`**: These fields specify autoscaler container
environment variables. These fields should be formatted following the
[Kuberentes API](https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/pod-v1/#environment-variables)
for container environment variables.

## Understanding the Ray Autoscaler in the Context of Kubernetes
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
