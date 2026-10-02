---
myst:
  html_meta:
    description: "Guidance for running Ray on Kubernetes that doesn't depend on the KubeRay operator, such as storage, dependencies, and container image pull latency."
---

(ray-on-kubernetes)=

# Ray on Kubernetes

```{toctree}
:hidden:

user-guides/storage
user-guides/reduce-image-pull-latency
```

This section covers guidance for running Ray on Kubernetes that doesn't depend on the KubeRay operator. Each Ray node runs as a Kubernetes Pod.

To deploy and manage Ray clusters on Kubernetes, use KubeRay, the officially supported operator. See {ref}`KubeRay <kuberay-index>`.

The following guides apply to Ray on Kubernetes in general:

* {ref}`kuberay-storage`
* {ref}`reduce-image-pull-latency`
