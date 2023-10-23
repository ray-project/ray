(kuberay-upgrade-guide)=

# KubeRay upgrade guide

## KubeRay / Ray compatibility

KubeRay CI tests the nightly KubeRay against the three most recent major or minor releases of Ray, as well as against the nightly Ray build.
For example, if the latest Ray release is 2.7.0, KubeRay CI tests the nightly KubeRay against Ray 2.7.0, 2.6.0, 2.5.0, and the nightly Ray build.

* KubeRay v0.6.0: Supports all Ray versions > Ray 2.0.0
* KubeRay v1.0.0: Supports all Ray versions > Ray 2.0.0
* KubeRay v1.1.0: Supports Ray 2.8.0 and later. Release planned with Ray 2.10.0. 

The preceding compatibility plan is closely tied to the KubeRay CRD versioning plan.

## CRD versioning

Typically, while new fields are added to the KubeRay CRD in each release, KubeRay doesn't bump the CRD version for every release.

* KubeRay v0.6.0 and older: CRD v1alpha1
* KubeRay v1.0.0: CRD v1alpha1 and v1
* KubeRay v1.1.0: CRD v1

If you want to understand the reasoning behind the CRD versioning plan, see [ray-project/ray#40357](https://github.com/ray-project/ray/pull/40357) for more details.

## Upgrade KubeRay

Upgrading the KubeRay version is the best strategy if you have any issues with KubeRay.

* Because a lot of users are unable to install Kubernetes webhooks due to their security policies, KubeRay doesn't provide a webhook for the CRD upgrade.
* If you plan to upgrade to KubeRay v1.0.0 or later, you may need to upgrade the `apiVersion` in your custom resource YAML files from `ray.io/v1alpha1` to `ray.io/v1`.
* Based on [the Helm documentation](https://helm.sh/docs/chart_best_practices/custom_resource_definitions/#some-caveats-and-explanations), there is no support at this time for upgrading or deleting CRDs using Helm.
  * If you want to install the latest KubeRay release's CRD, you may need to delete the old CRD first.
  * Note that deleting the CRD causes a cascading deletion of custom resources. See the [Helm documentation](https://github.com/helm/community/blob/main/hips/hip-0011.md#deleting-crds) for more details.
  ```sh
  kubectl delete crd rayclusters.ray.io
  kubectl delete crd rayjobs.ray.io
  kubectl delete crd rayservices.ray.io
  ```
