(kuberay-api-reference)=
# API Reference

To learn about RayCluster configuration, Ray recommends taking a look at
the {ref}`configuration guide <kuberay-config>`.

For comprehensive coverage of all supported RayCluster fields,
refer to the [API reference][APIReference].

## KubeRay API compatibility and guarantees

v1 APIs in the KubeRay project are stable and suitable for production environments.
Fields in the v1 APIs are never removed to maintain compatibility.
Future major versions of the API (such as v2) may have breaking changes and fields removed from v1.

However, KubeRay maintainers preserve the right to mark fields as deprecated and remove
features associated with deprecated fields after a minimum of two minor releases.
In addition, some definitions of the API may see small changes in behavior. For example,
the definition of a "ready" or "unhealthy" RayCluster could change to better handle new
failure scenarios.

[APIReference]: https://ray-project.github.io/kuberay/reference/api/
