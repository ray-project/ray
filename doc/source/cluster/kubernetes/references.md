---
myst:
  html_meta:
    description: "API reference for the KubeRay custom resources, plus KubeRay's API compatibility and stability guarantees."
---

(kuberay-api-reference)=
# API Reference

```{toctree}
:hidden:

references/api
```

To learn about RayCluster configuration, we recommend taking a look at the {ref}`configuration guide <kuberay-config>`.

For comprehensive coverage of all supported RayCluster fields, refer to the {ref}`KubeRay CRD API reference <kuberay-crd-api-reference>`. It documents every field of the `ray.io/v1` custom resources, and is generated from the KubeRay CRD definitions.

## KubeRay API compatibility and guarantees

v1 APIs in the KubeRay project are stable and suitable for production environments. Fields in the v1 APIs will never be removed to maintain compatibility. Future major versions of the API (i.e. v2) may have breaking changes and fields removed from v1.

However, KubeRay maintainers preserve the right to mark fields as deprecated and remove functionality associated with deprecated fields after a minimum of two minor releases. In addition, some definitions of the API may see small changes in behavior. For example, the definition of a "ready" or "unhealthy" RayCluster could change to better handle new failure scenarios.

The `ray.io/v1alpha1` API version is deprecated. It has not been the storage version since KubeRay v1.0, it receives no new fields, and it is slated for removal. Use `ray.io/v1` instead. The API reference covers `ray.io/v1` only.
