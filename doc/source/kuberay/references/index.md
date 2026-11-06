---
myst:
  html_meta:
    description: "API reference for the KubeRay custom resources, plus KubeRay's API compatibility and stability guarantees."
---

(kuberay-api-reference)=
# API Reference

```{toctree}
:hidden:

api
```

To learn about RayCluster configuration, we recommend taking a look at the {ref}`configuration guide <kuberay-config>`.

For comprehensive coverage of all supported RayCluster fields, refer to the {ref}`KubeRay CRD API reference <kuberay-crd-api-reference>`. It documents every field of the `ray.io/v1` custom resources, and is generated from the KubeRay CRD definitions.

## KubeRay API compatibility and guarantees

v1 APIs in the KubeRay project are stable and suitable for production environments. Fields in the v1 APIs will never be removed to maintain compatibility. Future major versions of the API (i.e. v2) may have breaking changes and fields removed from v1.

However, KubeRay maintainers preserve the right to mark fields as deprecated and remove functionality associated with deprecated fields after a minimum of two minor releases. In addition, some definitions of the API may see small changes in behavior. For example, the definition of a "ready" or "unhealthy" RayCluster could change to better handle new failure scenarios.

(kuberay-api-versions)=
### API versions

`ray.io/v1` is the current KubeRay API version. It's the storage version for the RayCluster, RayJob, RayService, and RayCronJob custom resources, and the preceding guarantees apply to it. Write new manifests against `ray.io/v1`. The {ref}`API reference <kuberay-crd-api-reference>` covers `ray.io/v1` only.

`ray.io/v1alpha1` is deprecated. It stopped being the storage version in KubeRay v1.0 and receives no new fields. Everything added since then exists only in `ray.io/v1`, such as the `authOptions` and `gcsFaultToleranceOptions` fields and the RayCronJob custom resource. The custom resource definitions declare no conversion webhook, so Kubernetes doesn't translate fields between the two versions. If you apply a `ray.io/v1alpha1` manifest that sets a field only `ray.io/v1` defines, the outcome depends on the client's field validation level. `kubectl` validates strictly by default, so the API server rejects the manifest. Clients that don't request a validation level get the API server default: it accepts the manifest, drops the field, and returns a warning.

KubeRay retires `ray.io/v1alpha1` in stages, so that manifests stop working only after advance warning:

1. KubeRay 1.7 marks the version deprecated. `kubectl` prints a deprecation warning for any request that uses `ray.io/v1alpha1`, such as `kubectl apply`.
1. KubeRay 1.9 stops serving the version, which is the release the deprecation warning names. From then on, the API server rejects `ray.io/v1alpha1` manifests.
1. A later release removes the version from the custom resource definitions.

To migrate, change the `apiVersion` of each manifest to `ray.io/v1`, then check its fields against the API reference.

Don't confuse `ray.io/v1alpha1` with `config.ray.io/v1alpha1`. That's a separate, current API group for the KubeRay operator's own configuration file, and the deprecation doesn't affect it.
