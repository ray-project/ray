(kuberay-upgrade-guide)=

# KubeRay upgrade guide

## KubeRay / Ray compatibility

KubeRay CI tests the nightly KubeRay against the three most recent major or minor releases of Ray, as well as against the nightly Ray build.
For example, if the latest Ray release is 2.7.0, KubeRay CI tests the nightly KubeRay against Ray 2.7.0, 2.6.0, 2.5.0, and the nightly Ray build.

```{admonition} Don't use Ray versions between 2.11.0 and 2.37.0.
The [commit](https://github.com/ray-project/ray/pull/44658) introduces a bug in Ray 2.11.0.
When a Ray job is created, the Ray dashboard agent process on the head node gets stuck, causing the readiness and liveness probes, which send health check requests for the Raylet to the dashboard agent, to fail.
```

* KubeRay v0.6.0: Supports all Ray versions > Ray 2.0.0
* KubeRay v1.0.0: Supports all Ray versions > Ray 2.0.0
* KubeRay v1.1.0: Supports Ray 2.8.0 and later.
* KubeRay v1.2.X: Supports Ray 2.8.0 and later.
* KubeRay v1.3.X: Supports Ray 2.38.0 and later.
* KubeRay v1.4.X: Supports Ray 2.38.0 and later.

The preceding compatibility plan is closely tied to the KubeRay CRD versioning plan.

## CRD versioning

Typically, while new fields are added to the KubeRay CRD in each release, KubeRay doesn't bump the CRD version for every release.

* KubeRay v0.6.0 and older: CRD v1alpha1
* KubeRay v1.0.0: CRD v1alpha1 and v1
* KubeRay v1.1.0 and later: CRD v1

If you want to understand the reasoning behind the CRD versioning plan, see [ray-project/ray#40357](https://github.com/ray-project/ray/pull/40357) for more details.

## Upgrade KubeRay

Upgrading the KubeRay version is the best strategy if you have any issues with KubeRay.
Due to reliability and security implications of webhooks, KubeRay doesn't support a conversion webhook to convert v1alpha1 to v1 APIs.

To upgrade the KubeRay version, follow these steps in order:
1. Upgrade the CRD manifest, containing new fields added to the v1 CRDs.
2. Upgrade the kuberay-operator image to the new version.
3. Verify the success of the upgrade.

The following is an example of upgrading KubeRay from v1.3.X to v1.4.0:
```
# Upgrade the CRD to v1.5.0.
# Note: This example uses kubectl because Helm doesn't support lifecycle management of CRDs.
# See the Helm documentation for more details: https://helm.sh/docs/chart_best_practices/custom_resource_definitions/#some-caveats-and-explanations
$ kubectl replace -k "github.com/ray-project/kuberay/ray-operator/config/crd?ref=v1.5.0"

# Upgrade kuberay-operator to v1.5.0. This step doesn't upgrade the CRDs.
$ helm upgrade kuberay-operator kuberay/kuberay-operator --version v1.5.0

# Install a RayCluster using the v1.5.0 helm chart to verify the success of the upgrade.
$ helm install raycluster kuberay/ray-cluster --version 1.5.0
```
