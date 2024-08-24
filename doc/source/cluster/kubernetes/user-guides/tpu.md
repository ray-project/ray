(kuberay-tpu)=

# Use TPUs with KubeRay

This document provides tips on TPU usage with KubeRay.

TPUs are available on Google Kubernetes Engine (GKE). To use TPUs with Kubernetes, configure
both the Kubernetes setup and add additional values to the RayCluster CR configuration.
Configure TPUs on GKE by referencing the {ref}`kuberay-gke-tpu-cluster-setup`.

## About TPUs

TPUs are custom-designed AI accelerators, which are optimized for training and inference of large AI models. A TPU host is a VM that runs on a physical computer connected to TPU hardware. TPU workloads can run on one or multiple hosts. A TPU Pod slice is a collection of chips all physically colocated and connected by high-speed inter chip interconnects (ICI). Single-host TPU Pod slices contain independent TPU VM hosts and communicate over the Data Center Network (DCN) rather than ICI interconnects. Multi-host TPU Pod slices contain two more interconnected TPU VM hosts. In GKE, multi-host TPU Pod slices run on their own node pools and GKE scales them atomically by node pools, rather than individual nodes. Ray enables single-host and multi-host TPU Pod slices to be scaled seamlessly to multiple slices, enabling greater parallelism to support larger workloads.

## Quickstart: Serve a Stable Diffusion model on GKE with TPUs

After setting up a GKE cluster with TPUs and the Ray TPU initialization webhook, run a workload on Ray with TPUs. {ref}`Serve a Stable Diffusion model on GKE with TPUs <kuberay-tpu-stable-diffusion-example>` shows how to serve a model with KubeRay on single-host TPUs.

## Configuring Ray Pods for TPU usage

Using any TPU accelerator requires specifying `google.com/tpu` resource `limits` and `requests` in the container fields of your `RayCluster`'s
`workerGroupSpecs`. This resource specifies the number of TPU chips for GKE to allocate each Pod. KubeRay v1.1.0 adds a `numOfHosts`
field to the RayCluster custom resource, specifying the number of TPU hosts to create per worker group replica. For multi-host worker groups,
Ray treats replicas as Pod slices rather than individual workers, and creates `numOfHosts` worker nodes per replica.
Additionally, GKE uses `gke-tpu` node selectors to schedule TPU Pods on the node matching the desired TPU accelerator and topology.

Below is a config snippet for a RayCluster worker group with 2 Ray TPU worker Pods. Ray schedules each worker on its own GKE v4 TPU node belonging to the same TPU Pod slice.

```
   groupName: tpu-group
   replicas: 1
   minReplicas: 0
   maxReplicas: 1
   numOfHosts: 2
   ...
   template:
       spec:
        ...
        containers:
         - name: ray-worker
           image: rayproject/ray:2.9.0-py310
           ...
           resources:
             google.com/tpu: "4" # Required to use TPUs.
             ...
           limits:
             google.com/tpu: "4" # The resources and limits value is expected to be equal.
             ...
        nodeSelector:
            cloud.google.com/gke-tpu-accelerator: tpu-v4-podslice
            cloud.google.com/gke-tpu-topology: 2x2x2
            ...
```

## TPU workload scheduling

After Ray deploys a Ray Pod with TPU resources, the Ray Pod can execute tasks and actors annotated with TPU requests.
Ray supports TPUs as a [custom resource](https://docs.ray.io/en/latest/ray-core/scheduling/resources.html#custom-resources).
Tasks or actors request TPUs using the decorator `@ray.remote(resources={"TPU": NUM_TPUS})`.

## Multi-Host TPU autoscaling

Multi-host TPU autoscaling is supported in Kuberay versions 1.1.0 or later and Ray versions 2.32.0 or later. Ray multi-host TPU worker groups are worker groups, which specify "google.com/tpu" Kubernetes container limits or requests and have `NumOfHosts` greater than 1. Ray treats each replica of a Ray multi-host TPU worker group as a TPU Pod slice and scales them atomically. When scaling up, multi-host worker groups create `NumOfHosts` Ray workers per replica. Likewise, Ray scales down multi-host worker group replicas by `NumOfHosts` workers. When Ray schedules a deletion of a single Ray worker in a multi-host TPU worker group, it terminates the entire replica to which the worker belongs. When scheduling TPU workloads on multi-host worker groups, ensure that Ray tasks or actors run on every TPU VM host in a worker group replica to avoid Ray from scaling down idle TPU workers.

Further reference and discussion
--------------------------------
* See [TPUs in GKE](https://cloud.google.com/kubernetes-engine/docs/how-to/tpus) for more details on using TPUs.
* [TPU availability](https://cloud.google.com/tpu/docs/regions-zones)
* [TPU System Architecture](https://cloud.google.com/tpu/docs/system-architecture-tpu-vm)
