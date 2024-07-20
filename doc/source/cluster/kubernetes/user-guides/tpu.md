(kuberay-tpu)=

# Using TPUs with KubeRay

This document provides tips on TPU usage with KubeRay.

TPUs are available on Google Kubernetes Engine (GKE). To use TPUs with Kubernetes, configure
both your Kubernetes setup and add additional values to your RayCluster configuration.
Instructions for configuring TPUs on GKE can be found here:

- {ref}`kuberay-gke-tpu-cluster-setup`

## Quickstart: Serve a Stable Diffusion model on GKE with TPUs

After setting up a GKE cluster with TPUs and the Ray TPU initialization webhook, you're ready to begin running
workloads on Ray with TPUs. The :ref:`StableDiffusion example <kuberay-tpu-stable-diffusion-example>` shows how to
serve a model with Ray on single-host TPUs.


## Configuring Ray Pods for TPU usage

Using any TPU accelerator requires specifying `google.com/tpu` resource `limits` and `requests` in the container fields of your `RayCluster`'s
`workerGroupSpecs`. This resource specifies the number of TPU chips for GKE to allocate each Pod. KubeRay v1.1.0 adds a `numOfHosts`
field to the RayCluster custom resource, specifying the number of TPU hosts to create per worker group replica. For multi-host worker groups,
replicas are treated as PodSlices rather than individual workers, with `numOfHosts` worker nodes being created per replica.
Additionally, GKE uses `gke-tpu` node selectors to schedule TPU pods on the node matching the desired TPU accelerator and topology.

Below is a config snippet for a RayCluster worker group with 2 Ray TPU worker pods, each scheduled on its own GKE v4 TPU node belonging to the same TPU PodSlice.

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
             cpu: "1"
             ephemeral-storage: 10Gi
             google.com/tpu: "4" # Required to use TPUs.
             memory: 40G
           limits:
             cpu: "1"
             ephemeral-storage: 10Gi
             google.com/tpu: "4" # The resources and limits value is expected to be equal.
             memory: 40G
        nodeSelector:
            cloud.google.com/gke-tpu-accelerator: tpu-v4-podslice
            cloud.google.com/gke-tpu-topology: 2x2x2
            ...
```

## TPU workload scheduling

After a Ray pod with with TPU pod resources is deployed, it will be able to execute tasks and actors annotated with TPU requests.
TPUs are supported on Ray as a [custom resource](https://docs.ray.io/en/latest/ray-core/scheduling/resources.html#custom-resources),
and are requested by tasks or actors using the decorator `@ray.remote(resources={"TPU": NUM_TPUS})`.


Further reference and discussion
--------------------------------
* See [TPUs in GKE](https://cloud.google.com/kubernetes-engine/docs/how-to/tpus) for more details on using TPUs.
* [TPU availability](https://cloud.google.com/tpu/docs/regions-zones)
* [nodeSelectors](https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#nodeselector)
