(ray-k8s-autoscaler-comparison)=
# (Advanced) Understanding the Ray Autoscaler in the Context of Kubernetes
We describe the relationship between the Ray autoscaler and other autoscalers in the Kubernetes
ecosystem.

## Ray Autoscaler vs. Horizontal Pod Autoscaler
The Ray autoscaler adjusts the number of Ray nodes in a Ray cluster.
On Kubernetes, each Ray node is run as a Kubernetes Pod. Thus in the context of Kubernetes,
the Ray autoscaler scales Ray **Pod quantities**. In this sense, the Ray autoscaler
plays a role similar to that of the Kubernetes
[Horizontal Pod Autoscaler](https://kubernetes.io/docs/tasks/run-application/horizontal-Pod-autoscale/) (HPA).
However, the following features distinguish the Ray Autoscaler from the HPA.

### Load metrics are based on application semantics
The Horizontal Pod Autoscaler determines scale based on physical usage metrics like CPU
and memory. By contrast, the Ray autoscaler uses the logical resources expressed in
task and actor annotations. For instance, if each Ray container spec in your RayCluster CR indicates
a limit of 10 CPUs, and you submit twenty tasks annotated with `@ray.remote(num_cpus=5)`,
10 Ray Pods are created to satisfy the 100-CPU resource demand.
In this respect, the Ray autoscaler is similar to the
[Kubernetes Cluster Autoscaler](https://github.com/kubernetes/autoscaler/tree/master/cluster-autoscaler),
which makes scaling decisions based on the logical resources expressed in container
resource requests.

### Fine-grained control of scale-down
To accommodate the statefulness of Ray applications, the Ray autoscaler has more
fine-grained control over scale-down than the Horizontal Pod Autoscaler. In addition to
determining desired scale, the Ray Autoscaler is able to select precisely which Pods
to scale down. The KubeRay operator then deletes that Pod.
By contrast, the Horizontal Pod Autoscaler can only decrease a replica count, without much
control over which Pods are deleted. For a Ray application, downscaling a random
Pod could be dangerous.

### Architecture: One Ray Autoscaler per Ray Cluster
Horizontal Pod Autoscaling is centrally controlled by a manager in the Kubernetes control plane;
the manager controls the scale of many Kubernetes objects.
By contrast, each Ray cluster is managed by its own Ray autoscaler process,
running as a sidecar container in the Ray head Pod. This design choice is motivated
by the following considerations:

- **Scalability.** Autoscaling each Ray cluster requires processing a significant volume of resource
  data from that Ray cluster.
- **Simplified versioning and compatibility.** The autoscaler and Ray are both developed
  as part of the Ray repository. The interface between the autoscaler and the Ray core is complex.
  To support multiple Ray clusters running at different Ray versions, it is thus best to match
  Ray and Autoscaler code versions. Running one autoscaler per Ray cluster and matching the code versions
  ensures compatibility.

(kuberay-autoscaler-with-ray-autoscaler)=
## Ray Autoscaler with Kubernetes Cluster Autoscaler
The Ray Autoscaler and the
[Kubernetes Cluster Autoscaler](https://github.com/kubernetes/autoscaler/tree/master/cluster-autoscaler)
complement each other.
After the Ray autoscaler decides to create a Ray Pod, the Kubernetes Cluster Autoscaler
can provision a Kubernetes node so that the Pod can be placed.
Similarly, after the Ray autoscaler decides to delete an idle Pod, the Kubernetes
Cluster Autoscaler can clean up the idle Kubernetes node that remains.
It is recommended to configure your RayCluster so that only one Ray Pod fits per Kubernetes node.
If you follow this pattern, Ray Autoscaler Pod scaling events correspond roughly one-to-one with cluster autoscaler
node scaling events. (We say "roughly" because it is possible for a Ray Pod be deleted and replaced
with a new Ray Pod before the underlying Kubernetes node is scaled down.)


## Vertical Pod Autoscaler
There is no relationship between the Ray Autoscaler and the Kubernetes
[Vertical Pod Autoscaler](https://github.com/kubernetes/autoscaler/tree/master/vertical-pod-autoscaler) (VPA),
which is meant to size individual Pods to the appropriate size based on current and past usage.
If you find that the load on your individual Ray Pods is too high, there are a number
of manual techniques to decrease the load.
One method is to schedule fewer tasks/actors per node by increasing the resource
requirements specified in the `ray.remote` annotation.
For example, changing `@ray.remote(num_cpus=2)` to `@ray.remote(num_cpus=4)`
will halve the quantity of that task or actor that can fit in a given Ray Pod.
