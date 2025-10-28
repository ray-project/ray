(kuberay-label-scheduling)=

# KubeRay label-based scheduling

This guide explains how to use label-based scheduling for Ray clusters on Kubernetes. This feature allows you to direct Ray workloads (tasks, actors, or placement groups) to specific Ray nodes running on Pods using labels. Label selectors enable fine-grained control of where your workloads run in a heterogeneous cluster, helping to optimize both performance and cost.

Label-based scheduling is an essential tool for heterogeneous clusters, where your RayCluster might contain different types of nodes for different purposes, such as:

* Nodes with different accelerator types like A100 GPUs or Trillium TPU.
* Nodes with different CPU families like Intel or AMD.
* Nodes with different instance types related to cost and availability, such as spot or on-demand instances.
* Nodes in different failure domains or with region or zone requirements.

The Ray scheduler uses a `label_selector` specified in the `@ray.remote` decorator to filter on labels defined on the Ray nodes. In KubeRay, set Ray node labels using labels defined in the RayCluster custom resource.


```{admonition} Label selectors are an experimental feature in Ray 2.49.1.
Full autoscaling support for tasks, actors, and placement groups with label selectors is available in Ray 2.51.0 and KubeRay v1.5.0.
```

## Overview

There are three scheduling steps to understand when using KubeRay with label-based scheduling:
1. **The Ray workload**: A Ray application requests resources with a `label_selector`, specifying that you want to schedule on a node with those labels. Example:
```py
@ray.remote(num_gpus=1, label_selector={"ray.io/accelerator-type": "A100"})
def gpu_task():
    pass
```

2. **The RayCluster CR**: The RayCluster CRD defines the types of nodes available for scheduling (or scaling with autoscaling) through `HeadGroupSpec` and `WorkerGroupSpecs`. To set Ray node labels for a given group, you can specify them under a top-level `Labels` field. When KubeRay creates a Pod for this group, it sets these labels in the Ray runtime environment. For RayClusters with autoscaling enabled, KubeRay also adds these labels to the autoscaling configuration use for scheduling Ray workloads. Example:
```yaml
headGroupSpec:
    labels:
        ray.io/region: us-central2
...
workerGroupSpecs:
  - replicas: 1
    minReplicas: 1
    maxReplicas: 10
    groupName: intel-cpu-group
    labels:
      cpu-family: intel
      ray.io/market-type: on-demand
```

3. **The Kubernetes scheduler**: To ensure the Ray Pods land on the correct physical hardware, add standard Kubernetes scheduling features like `nodeSelector` or `podAffinity` in the Pod template. Similar to how Ray treats label selectors, the Kubernetes scheduler filters the underlying nodes in the Kubernetes cluster based on these labels when scheduling the Pod. For example, you might add the following `nodeSelector` to the above `intel-cpu-group` to ensure both Ray and Kubernetes constrain scheduling:
```yaml
nodeSelector:
    cloud.google.com/machine-family: "N4"
    cloud.google.com/gke-spot: "false"
```

This quickstart demonstrates all three steps working together.

## Quickstart

### Step 1: [Optional] Create a Kubernetes cluster with Kind

If you don't already have a Kubernetes cluster, create a new cluster with Kind for testing. If you're already using a cloud provider's Kubernetes service such as GKE, skip this step.

```bash
kind create cluster --image=kindest/node:v1.26.0

# Mock underlying nodes with GKE-related labels. This is necessary for the `nodeSelector` to be able to schedule Pods.
kubectl label node kind-control-plane \
  cloud.google.com/machine-family="N4" \
  cloud.google.com/gke-spot="true" \
  cloud.google.com/gke-accelerator="nvidia-tesla-a100"
```

```{admonition} This quickstart uses Kind for simplicity.
In a real-world scenario, you would use a cloud provider's Kubernetes service (like GKE or EKS) that has different machine types, like GPU nodes and spot instances, available.
```

### Step 2: Install the KubeRay operator

Follow [this document](kuberay-operator-deploy) to install the latest stable KubeRay operator via Helm repository. The minimum KubeRay version for this guide is v1.5.0.

### Step 3: Create a RayCluster CR with autoscaling enabled and labels specified

```bash
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-cluster-label-selector.yaml
```

### Step 4: Verify the Kubernetes cluster status

```bash
# Step 4.1: List all Ray Pods in the `default` namespace.
kubectl get pods -l=ray.io/is-ray-node=yes

# [Example output]
NAME                                             READY   STATUS     RESTARTS   AGE
ray-label-cluster-head-5tkn2                     2/2     Running    0          3s
ray-label-cluster-large-cpu-group-worker-dhqmt   1/1     Running    0          3s

# Step 4.2: Check the ConfigMap in the `default` namespace.
kubectl get configmaps

# [Example output]
# NAME                  DATA   AGE
# ray-example           3      21s
# ...
```

The RayCluster has 1 head Pod and 1 worker Pod already scaled. The head Pod has two containers: a Ray head container and a Ray autoscaler sidecar container. Additionally, the [ray-cluster-label-selector.yaml](https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-cluster-label-selector.yaml) includes a ConfigMap named `ray-example` that contains three Python scripts: `example_task.py`, `example_actor.py`, and `example_placement_group.py`, which all showcase label-based scheduling.

* `example_task.py` is a Python script that creates a simple task requiring a node with the `ray.io/market-type: on-demand` and `cpu-family: in(intel,amd)` labels. The `in` operator expresses that the cpu-family can be either Intel or AMD.
```py
import ray
@ray.remote(num_cpus=1, label_selector={"ray.io/market-type": "on-demand", "cpu-family": "in(intel,amd)"})
def test_task():
  pass
ray.init()
ray.get(test_task.remote())
```

* `example_actor.py` is a Python script that creates a simple actor requiring a node with the`ray.io/accelerator-type: A100` label. Ray sets the `ray.io/accelerator-type` label by default when it can detect the underlying compute.
```py
import ray
@ray.remote(num_gpus=1, label_selector={"ray.io/accelerator-type": "A100"})
class Actor:
  def ready(self):
    return True
ray.init()
my_actor = Actor.remote()
ray.get(my_actor.ready.remote())
```

* `example_placement_group.py` is a Python script that creates a placement group requiring two bundles of 1 CPU with the `ray.io/market-type: spot` label but NOT `ray.io/region: us-central2`. Since the strategy is "SPREAD", we expect two separate Ray nodes with the desired labels to scale up, one node for each placement group bundle.
```py
import ray
from ray.util.placement_group import placement_group
ray.init()
pg = placement_group(
  [{"CPU": 1}] * 2,
  bundle_label_selector=[{"ray.io/market-type": "spot", "ray.io/region": "!us-central2"},] * 2, strategy="SPREAD"
)
ray.get(pg.ready())
```

### Step 5: Trigger RayCluster label-based scheduling

```bash
# Step 5.1: Get the head pod name
export HEAD_POD=$(kubectl get pods --selector=ray.io/node-type=head -o custom-columns=POD:metadata.name --no-headers)

# Step 5.2: Run the task. The task should target the existing large-cpu-group and not require autoscaling.
kubectl exec -it $HEAD_POD -- python3 /home/ray/samples/example_task.py

# Step 5.3: Run the actor. This should cause the Ray autoscaler to scale a GPU node in accelerator-group. The Pod may not 
#           schedule unless you have GPU resources in your cluster.
kubectl exec -it $HEAD_POD -- python3 /home/ray/samples/example_actor.py

# Step 5.4: Create the placement group. This should cause the Ray autoscaler to scale two nodes in spot-group.
kubectl exec -it $HEAD_POD -- python3 /home/ray/samples/example_placement_group.py

# Step 5.5: List all nodes in the Ray cluster. The nodes scaled for the task, actor, and placement group should be annotated with
#           the expected Ray node labels.
kubectl exec -it $HEAD_POD -- ray list nodes
```

### Step 6: Clean up the Kubernetes cluster

```bash
# Delete RayCluster and ConfigMap
kubectl delete -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-cluster-label-selector.yaml

# Uninstall the KubeRay operator
helm uninstall kuberay-operator

# Delete the kind cluster
kind delete cluster
```

## Next steps
* See [Use labels to control scheduling](https://docs.ray.io/en/master/ray-core/scheduling/labels.html) for more details on label selectors in Ray.
* See [KubeRay Autoscaling](https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/configuring-autoscaling.html) for instructions on how to configure the Ray autoscaler with KubeRay.
