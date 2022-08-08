(kuberay-vs-legacy)=

# KubeRay vs. the legacy Ray Operator

Using the [KubeRay operator](https://ray-project.github.io/kuberay/components/operator/)
is the preferred way to deploy Ray on Kubernetes.
This page compares the KubeRay operator to the {ref}`legacy Ray Operator<ray-k8s-deploy>` hosted in the Ray repo.
This page also provides migration notes for users switching to the KubeRay operator.

## KubeRay vs. Legacy Ray Operator: Similarities

### Purpose
The two operators have the same purpose: managing clusters of Ray pods deployed on Kubernetes.

### High-level interface structure
Both operators rely on a user-specified custom resource specifying Ray pod configuration and
Ray worker pod quantities.

## KubeRay vs. Legacy Ray Operator: Differences

The two operators differ primarily in internal design and implementation.
There are also some differences in configuration details.

### Implementation and architecture
**Legacy Ray Operator** The legacy Ray Operator is implemented in Python.
Kubernetes event handling is implemented using the [Kopf](https://kopf.readthedocs.io/en/stable/) framework.
The operator invokes Ray cluster launcher and autoscaler code to manage Ray clusters.
The operator forks an autoscaler subprocess for each Ray cluster it manages.
The Ray autoscaler subprocesses create and delete Ray pods directly.

**KubeRay Operator** The KubeRay operator is implemented in Golang using standard tools
for building Kubernetes operators, including the [KubeBuilder](https://github.com/kubernetes-sigs/kubebuilder)
operator framework
and the [client-go](https://github.com/kubernetes/client-go) client library.
The KubeRay operator is structurally simpler than the legacy Ray Operator;
rather than running many Ray autoscalers in subprocesses, the KubeRay operator implements a simple
reconciliation loop. The reconciliation loop creates and deletes Ray pods to match the desired
state expressed in each RayCluster CR.
Each Ray cluster runs its own autoscaler as a sidecar to the Ray head pod.
The Ray autoscaler communicates desired scale to the KubeRay operator by writing to the RayCluster
custom resource.

### Scalability
The KubeRay operator is more scalable than the legacy Ray Operator. Specifically, the
KubeRay operator can simultaneously manage more Ray clusters.

**Legacy Ray Operator** Each Ray autoscaler consumes nontrivial memory and CPU resources.
Since the legacy Ray Operator runs many autoscalers in one pod, it cannot manage many Ray clusters.

**KubeRay Operator** The KubeRay operator does not run Ray autoscaler processes.
Each Ray autoscaler runs as a sidecar to the Ray head. Since managing each Ray cluster is cheap,
the KubeRay operator can manage many Ray clusters.

### Ray version compatibility

**Legacy Ray Operator**
It is recommended to use the same Ray version in the legacy Ray operator
as in all of the Ray pods managed by the operator.
Matching Ray versions is required to maintain compatibility between autoscaler code
running in the operator pod and Ray code running in the Ray cluster.

**KubeRay Operator**
The KubeRay operator is compatible with many Ray versions.
Compatibility of KubeRay v0.3.0 with Ray versions 1.11, 1.12, 1.13, and 2.0 is tested explicitly.

Note however that autoscaling with KubeRay is supported only with Ray versions
at least as new as 1.11.0.

### Configuration details
Some details of Ray cluster configuration are different with KubeRay; see the next section
for migration notes. Refer to the {ref}`configuration guide<kuberay-config>` for comprehensive
information.

## Migration Notes

Take note of the following configuration differences when switching to KubeRay
deployment.

### Autoscaling is optional
Ray Autoscaler support is optional with KubeRay. Set `spec.enableInTreeAutoscaling:true`
in the RayCluster CR to enable autoscaling. The KubeRay operator will then automatically
configure a Ray Autoscaler sidecar for the Ray head pod.
The autoscaler container requests 500m CPU and 512Mi memory by default.
Autoscaler container configuration is accessible via `spec.autoscalerOptions`.
Note that autoscaling with KubeRay is supported only with Ray versions at least as new 1.11.

### No need to specify /dev/shm
The KubeRay operator automatically configures a `/dev/shm` volume for each Ray pod's object store.
There is no need to specify this volume in the RayCluster CR.

### Namespace-scoped operation.
Similar to the legacy Ray Operator, it is possible to run the KubeRay operator at single-namespace scope.
See the [KubeRay documentation][KubeRaySingleNamespace] for details.

Note that the KubeRay operator can manage many Ray clusters running at different Ray versions.
Thus, from a scalability and compatibility perspective, there is no need to run
one KubeRay operator per Kubernetes namespace. Run a namespace-scoped KubeRay operator
only if necessary, e.g. to accommodate permissions constraints in your Kubernetes cluster.

### Specifying resource quantities.
Ray pod CPU, GPU, and memory capacities are detected from container resource limits and advertised
to Ray.

The interface for overriding the resource capacities advertised to Ray is different:
Resource overrides must be specified in `rayStartParams`.
For example, you may wish to prevent the Ray head pod
from running Ray workloads by labelling the head as having 0 CPU capacity.
To achieve this with KubeRay, include the following in the `headGroupSpec`'s configuration:
```yaml
rayStartParams:
    num-cpus: "0"
```
To advertise custom resource capacities to Ray, one uses the field `rayStartParams.resources`.
See the {ref}`configuration guide<rayStartParams>` for details.

[KuberaySingleNamespace]: https://github.com/ray-project/kuberay#single-namespace-version

### Init Containers and Pre-Stop hooks
Pass...
