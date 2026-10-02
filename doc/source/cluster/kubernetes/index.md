---
myst:
  html_meta:
    description: "Run Ray on Kubernetes: choose an operator, KubeRay or the Anyscale operator, and find guidance that applies however you deploy, such as storage, dependencies, and container image pull latency."
---

(ray-on-kubernetes)=

# Ray on Kubernetes

```{toctree}
:hidden:

user-guides/storage
user-guides/reduce-image-pull-latency
```

This section covers running Ray on Kubernetes in general. Each Ray node runs as a Kubernetes Pod.

To deploy and manage Ray clusters on Kubernetes, use one of the following operators:

* **KubeRay**: The officially supported open source Kubernetes operator for Ray. See {ref}`kuberay-index`.
* **Anyscale operator**: [Anyscale](https://www.anyscale.com/?utm_source=ray_docs&utm_medium=docs&utm_campaign=ray-doc-upsell&utm_content=ray-on-kubernetes) is the managed Ray platform developed by the creators of Ray. The Anyscale operator for Kubernetes runs in your Kubernetes cluster and deploys Ray nodes as Pods on instructions from the Anyscale control plane. See [Anyscale on Kubernetes](https://docs.anyscale.com/clouds/kubernetes?utm_source=ray_docs&utm_medium=docs&utm_campaign=ray-doc-upsell&utm_content=ray-on-kubernetes).

The following guides apply to Ray on Kubernetes in general:

* {ref}`kuberay-storage`
* {ref}`reduce-image-pull-latency`
