---
myst:
  html_meta:
    description: "Run Ray on Kubernetes: find the operator that deploys and manages your Ray clusters, and guides that don't depend on KubeRay, such as storage, dependencies, and container image pull latency."
---

(ray-on-kubernetes)=

# Ray on Kubernetes

This section covers Ray on Kubernetes topics that sit outside the KubeRay docs. Each Ray node runs as a Kubernetes Pod.

To deploy and manage Ray clusters on Kubernetes, use one of the following operators:

* **KubeRay**: The officially supported open source Kubernetes operator for Ray. See {ref}`kuberay-index`.
* **Anyscale operator**: The Kubernetes operator for Anyscale, the managed Ray platform. See [Anyscale on Kubernetes](https://docs.anyscale.com/clouds/kubernetes?utm_source=ray_docs&utm_medium=docs&utm_campaign=ray-doc-upsell&utm_content=ray-on-kubernetes).

The following guides don't depend on KubeRay:

* {ref}`kuberay-storage`
* {ref}`reduce-image-pull-latency`
