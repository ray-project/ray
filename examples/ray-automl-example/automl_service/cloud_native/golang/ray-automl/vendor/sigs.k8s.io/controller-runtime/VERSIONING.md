# Versioning and Branching in controller-runtime

We follow the [common KubeBuilder versioning guidelines][guidelines], and
use the corresponding tooling.

For the purposes of the aforementioned guidelines, controller-runtime
counts as a "library project", but otherwise follows the guidelines
exactly.

[guidelines]: https://sigs.k8s.io/kubebuilder-release-tools/VERSIONING.md

## Compatiblity and Release Support

For release branches, we generally tend to support backporting one (1)
major release (`release-{X-1}` or `release-0.{Y-1}`), but may go back
further if the need arises and is very pressing (e.g. security updates).

### Dependency Support

Note the [guidelines on dependency versions][dep-versions].  Particularly:

- We **DO** guarantee Kubernetes REST API compability -- if a given
  version of controller-runtime stops working with what should be
  a supported version of Kubernetes, this is almost certainly a bug.

- We **DO NOT** guarantee any particular compability matrix between
  kubernetes library dependencies (client-go, apimachinery, etc); Such
  compability is infeasible due to the way those libraries are versioned.

[dep-versions]: https://sigs.k8s.io/kubebuilder-release-tools/VERSIONING.md#kubernetes-version-compatibility
