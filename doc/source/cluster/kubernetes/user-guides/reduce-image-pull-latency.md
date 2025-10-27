(reduce-image-pull-latency)=

# Reducing image pull latency on Kubernetes

This guide outlines strategies to reduce image pull latency for Ray clusters on Kubernetes. Some of these strategies are provider-agnostic so you can use them on any Kubernetes cluster, while others leverage capabilities specific to certain cloud providers.

## Image pull latency

Ray container images can often be several gigabytes, primarily due to the Python dependencies included.
Other factors can also contribute to image size. Pulling large images from remote repositories can slow down Ray cluster startup times. The time required to download an image depends on several factors, including:
*   Whether image layers are already cached on the node.
*   The overall size of the image.
*   The reliability and throughput of the remote repository.

## Strategies for reducing image pulling latency

The following sections discuss strategies for reducing image pull latency.

### Preload images on every node using a DaemonSet

You can ensure that your Ray images are always cached on every node by running a DaemonSet that pre-pulls the images. This approach ensures that Ray downloads the image to each node, reducing the time to pull the image when a Ray needs to schedule a pod.

The following is an example DaemonSet configuration that uses the image `rayproject/ray:2.40.0`:
```yaml
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: ray-image-preloader
  labels:
    k8s-app: ray-image-preloader
spec:
  selector:
    matchLabels:
      k8s-app: ray-image-preloader
  template:
    metadata:
      labels:
        name: ray-image-preloader
        k8s-app: ray-image-preloader
    spec:
      containers:
      - image: rayproject/ray:2.40.0
        name: ray-image-preloader
        command: [ "sleep", "inf" ]
```

> **Note:** Ensure that the image tag that you use in the Daemonset is consistent with the Ray images your Ray cluster uses.

### Preload images into machine images

Some cloud providers allow you to build custom machine images for your Kubernetes nodes. Including your Ray images in these custom machine images ensures that Ray caches images locally when your nodes start up, avoiding the need to pull them from a remote registry. While this approach can be effective, it's generally not recommended, as changing machine images often requires multiple steps and is tightly coupled to the lifecycle of your nodes.

### Use private image registries

For production environments, it's generally recommended to avoid pulling images from the public internet. Instead, host your images closer to your cluster to reduce pull times. Cloud providers like Google Cloud and AWS offer services such as Artifact Registry (AR) and Elastic Container Registry (ECR), respectively. Using these services ensures that traffic for image pulls remains within the provider's internal network, avoiding network hops on the public internet and resulting in faster pull times.

### Enable Image streaming (GKE only)

If you're using Google Kubernetes Engine (GKE), you can leverage [Image streaming](https://cloud.google.com/kubernetes-engine/docs/how-to/image-streaming).

With Image streaming, GKE uses a remote filesystem as the root filesystem for any containers that use eligible container images.
GKE streams image data from the remote filesystem as needed by your workloads. While streaming the image data, GKE downloads the entire container image onto the local disk in the background and caches it.
GKE then serves future data read requests from the cached image. When you deploy workloads that need to read specific files in the container image, the Image streaming backend serves only those requested files.

Only container images hosted on [Artifact Registry](https://cloud.google.com/artifact-registry/docs/overview) are eligible for Image streaming.

> **Note:** You might not notice the benefits of Image streaming during the first pull of an eligible image. However, after Image streaming caches the image, future image pulls on any cluster benefit from Image streaming.

You can enable Image streaming when creating a GKE cluster by setting the `--enable-image-streaming` flag:
```
gcloud container clusters create CLUSTER_NAME \
    --zone=COMPUTE_ZONE \
    --image-type="COS_CONTAINERD" \
    --enable-image-streaming
```

See [Enable Image streaming on clusters](https://cloud.google.com/kubernetes-engine/docs/how-to/image-streaming#enable_on_clusters) for more details.

### Enable secondary boot disks (GKE only)

If you're using Google Kubernetes Engine (GKE), you can enable the [secondary bootdisk to preload data or container images](https://cloud.google.com/kubernetes-engine/docs/how-to/data-container-image-preloading).

GKE enables secondary boot disks per node pool. Once enabled, GKE attaches a Persistent Disk to each node within the node pool.
The images within the Persistent Disk are immediately accessible to containers once Ray schedules workloads on those nodes.
Including Ray images in the secondary boot disk can significantly reduce image pull latency.

See [Prepare the secondary boot disk image](https://cloud.google.com/kubernetes-engine/docs/how-to/data-container-image-preloading#prepare) for detailed steps on how to prepare the secondary boot disk. See [Configure the secondary boot disk](https://cloud.google.com/kubernetes-engine/docs/how-to/data-container-image-preloading#configure) for how to enable secondary boot disks for your node pools.
