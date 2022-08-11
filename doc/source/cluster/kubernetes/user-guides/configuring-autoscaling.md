(kuberay-autoscaling)=

# Configuring Autoscaling with KubeRay
This page discusses autoscaling in the context of Ray on Kubernetes.
For details on autoscaler configuration see the {ref}`configuration guide<kuberay-autoscaling-config>`.

## Getting Started with Autoscaling

First, follow the [quickstart guide](kuberay-quickstart) to create an autoscaling cluster. The commands to create the KubeRay operator and deploy an autoscaling cluster are summarized here:

```bash
# Optionally use kind to run the examples locally.
# kind create cluster

$ git clone https://github.com/ray-project/kuberay -b release-0.3
# Create the KubeRay operator.
$ kubectl create -k kuberay/ray-operator/config/default
# Create an autoscaling Ray cluster.
$ kubectl apply -f kuberay/ray-operator/config/samples/ray-cluster.autoscaler.yaml
```

Now, we can run a Ray program on the head pod that asks the autoscaler to scale the cluster to a total of 3 CPUs. The head and worker in our example cluster each have a capacity of 1 CPU, so the request should trigger upscaling of an additional worker pod.

Note that in real-life scenarios, you will want to use larger Ray pods. In fact, it is advantageous to size each Ray pod to take up an entire Kubernetes node. See the [configuration guide](kuberay-config) for more details.

To run the Ray program, we will first get the name of the Ray head pod:

```bash
$ kubectl get pods --selector=ray.io/cluster=raycluster-autoscaler --selector=ray.io/node-type=head -o custom-columns=POD:metadata.name --no-headers
# raycluster-autoscaler-head-xxxxx
```

Then, we can run the Ray program using ``kubectl exec``:
```bash
$ kubectl exec raycluster-autoscaler-head-xxxxx -it -c ray-head -- python -c \"import ray; ray.init(); ray.autoscaler.sdk.request_resources(num_cpus=3)
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
